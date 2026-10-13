import os, threading, time, requests, psycopg2
from flask import Flask, jsonify, request, send_from_directory

DEX, RPC, RUG = "https://api.dexscreener.com", "https://api.mainnet-beta.solana.com", "https://api.rugcheck.xyz/v1"

# --- Estratégia (v18) ---
MIN_SCORE, STOP, ALVO, TIMEOUT = 80, -15, 50, 6 * 3600
LIQ_QUEDA = 25
MIN_AMOSTRAS, MIN_ACERTO, LIQ_MIN = 100, 75, 50000
INTERVALO_SCAN, INTERVALO_POS = 90, 10
CAPITAL_USD, POSICAO_USD, LIMITE_PERDA_DIA_USD, MAX_AUTO_SIMULTANEAS = 10.0, 2.0, 3.0, 3

TRAIL_TIERS = [(100, 5), (50, 7), (30, 10)]
PARCIAL_PCT = 0.5

IDADE_MIN_H = 1.5
VOL_LIQ_MAX = 10
COOLDOWN_PERDAS, COOLDOWN_MIN = 2, 30

# --- Postgres / Render ---
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não definida. Configura-a nas env vars do Render.")

app = Flask(__name__, static_folder="static")
lock = threading.Lock()

def _connect():
    return psycopg2.connect(DATABASE_URL)

db = _connect()

def q(sql, a=()):
    """Executa SQL com placeholders %s. Reconecta uma vez se a ligação cair em idle."""
    global db
    with lock:
        try:
            cur = db.cursor()
            cur.execute(sql, a)
            try:
                r = cur.fetchall()
            except psycopg2.ProgrammingError:
                r = []
            db.commit()
            cur.close()
            return r
        except psycopg2.InterfaceError:
            try:
                db = _connect()
                cur = db.cursor()
                cur.execute(sql, a)
                try:
                    r = cur.fetchall()
                except psycopg2.ProgrammingError:
                    r = []
                db.commit()
                cur.close()
                return r
            except Exception:
                db.rollback()
                raise
        except Exception:
            try: db.rollback()
            except Exception: pass
            raise

# --- Schema ---
for stmt in (
    """CREATE TABLE IF NOT EXISTS sinais(
        id SERIAL PRIMARY KEY, addr TEXT UNIQUE, sym TEXT, score INTEGER,
        t0 DOUBLE PRECISION, p0 DOUBLE PRECISION, liq0 DOUBLE PRECISION, peak DOUBLE PRECISION,
        estado TEXT DEFAULT 'aberto', pnl DOUBLE PRECISION, motivo TEXT,
        parcial_feita INTEGER DEFAULT 0, pnl_parcial DOUBLE PRECISION DEFAULT 0)""",
    """CREATE TABLE IF NOT EXISTS posicoes(
        id SERIAL PRIMARY KEY, addr TEXT, sym TEXT, p0 DOUBLE PRECISION, liq0 DOUBLE PRECISION,
        peak DOUBLE PRECISION, t0 DOUBLE PRECISION, ativa INTEGER DEFAULT 1)""",
    """CREATE TABLE IF NOT EXISTS auto(
        id SERIAL PRIMARY KEY, addr TEXT, sym TEXT, p0 DOUBLE PRECISION, qtd DOUBLE PRECISION,
        liq0 DOUBLE PRECISION, peak DOUBLE PRECISION, t0 DOUBLE PRECISION, ativa INTEGER DEFAULT 1,
        pnl_usd DOUBLE PRECISION, motivo TEXT, dia TEXT,
        gas_usd DOUBLE PRECISION DEFAULT 0, custo_pct DOUBLE PRECISION DEFAULT 1.0,
        parcial_feita INTEGER DEFAULT 0, pnl_parcial DOUBLE PRECISION DEFAULT 0,
        custo_entrada_usd DOUBLE PRECISION DEFAULT 2.0)""",
    """CREATE TABLE IF NOT EXISTS curva(
        id SERIAL PRIMARY KEY, ts DOUBLE PRECISION, saldo DOUBLE PRECISION)""",
):
    q(stmt)

# Migrações suaves (idempotentes)
for tab, col, tipo in (
    ("auto", "gas_usd", "DOUBLE PRECISION DEFAULT 0"),
    ("auto", "custo_pct", "DOUBLE PRECISION DEFAULT 1.0"),
    ("auto", "parcial_feita", "INTEGER DEFAULT 0"),
    ("auto", "pnl_parcial", "DOUBLE PRECISION DEFAULT 0"),
    ("auto", "custo_entrada_usd", "DOUBLE PRECISION DEFAULT 2.0"),
    ("sinais", "parcial_feita", "INTEGER DEFAULT 0"),
    ("sinais", "pnl_parcial", "DOUBLE PRECISION DEFAULT 0"),
):
    try:
        q(f"ALTER TABLE {tab} ADD COLUMN {col} {tipo}")
    except Exception:
        pass

estado = {"cands": [], "rejeitados": [], "atualizado": 0, "erro": ""}
ultimo, seg_cache, ausencias = {}, {}, {}
perdas = {"n": 0, "ts": 0.0}


def get(url, tentativas=2):
    erro = None
    for i in range(tentativas):
        try:
            return requests.get(url, timeout=20).json()
        except Exception as e:
            erro = e
            if i < tentativas - 1:
                time.sleep(2)
    raise erro


def rpc(m, params):
    r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": m, "params": params}, timeout=20)
    return r.json()["result"]


def melhores(addrs):
    m = {}
    for i in range(0, len(addrs), 30):
        try:
            lote = get(f"{DEX}/latest/dex/tokens/{','.join(addrs[i:i+30])}").get("pairs") or []
        except Exception:
            continue
        for p in lote:
            a, l = p["baseToken"]["address"], (p.get("liquidity") or {}).get("usd", 0)
            if p["chainId"] == "solana" and (a not in m or l > (m[a].get("liquidity") or {}).get("usd", 0)):
                m[a] = p
    return m


_gas_cache = {"ts": 0, "usd_ida": 0.0}
SOL_MINT = "So11111111111111111111111111111111111111112"
CU_ESTIMADO_SWAP, ASSINATURAS_POR_TX = 200000, 1


def preco_sol():
    try:
        pares = [p for p in (get(f"{DEX}/latest/dex/tokens/{SOL_MINT}").get("pairs") or [])
                 if p["chainId"] == "solana"]
        return float(sorted(pares, key=lambda p: -(p.get("liquidity") or {}).get("usd", 0))[0]["priceUsd"])
    except Exception:
        return 150.0


def gas_usd_ida_volta():
    agora = time.time()
    if agora - _gas_cache["ts"] < 300 and _gas_cache["usd_ida"]:
        return _gas_cache["usd_ida"] * 2
    usd_ida = _gas_cache["usd_ida"] or 0.01
    try:
        fees = rpc("getRecentPrioritizationFees", [[]]) or []
        valores = sorted(f["prioritizationFee"] for f in fees if f.get("prioritizationFee") is not None)
        micro_lamports_cu = valores[len(valores) // 2] if valores else 0
        lamports_priority = micro_lamports_cu * CU_ESTIMADO_SWAP / 1e6
        lamports_base = 5000 * ASSINATURAS_POR_TX
        sol_gasto = (lamports_base + lamports_priority) / 1e9
        usd_ida = sol_gasto * preco_sol()
        _gas_cache.update(ts=agora, usd_ida=usd_ida)
    except Exception:
        pass
    return usd_ida * 2


JUP_ENDPOINTS = [
    "https://lite-api.jup.ag/swap/v1",
    "https://quote-api.jup.ag/v6",
]
CUSTO_SWAP_PADRAO_PCT = 0.6


def custo_pct_total():
    return gas_usd_ida_volta() / POSICAO_USD * 100 + CUSTO_SWAP_PADRAO_PCT


def custo_swap_pct_ida(mint):
    lamports = max(1, int(POSICAO_USD / preco_sol() * 1e9))
    for base in JUP_ENDPOINTS:
        try:
            d = get(f"{base}/quote?inputMint={SOL_MINT}&outputMint={mint}"
                    f"&amount={lamports}&slippageBps=150")
            if not isinstance(d, dict) or "routePlan" not in d:
                continue
            impacto = abs(float(d.get("priceImpactPct", 0) or 0)) * 100
            taxas = sum(float((f or {}).get("feeAmount", 0) or 0) for f in d.get("routePlan") or [])
            taxa_pct = (taxas / lamports * 100) if lamports else 0
            return impacto + taxa_pct
        except Exception as e:
            print(f"[jupiter] {base} falhou: {e}")
            continue
    return None


TTL_SEG_OK, TTL_SEG_FALHA = 600, 45


def seguranca(mint):
    c = seg_cache.get(mint)
    if c:
        r0, ts = c
        if time.time() - ts < (TTL_SEG_OK if r0 is not None else TTL_SEG_FALHA):
            return r0
    r = None
    try:
        d = get(f"{RUG}/tokens/{mint}/report")
        if not isinstance(d, dict) or "mint" not in d:
            raise ValueError("resposta RugCheck vazia/inválida (ex: rate-limit) — não confiar")
        risks = d.get("risks") or []
        perigo = [x.get("name", "") for x in risks if str(x.get("level", "")).lower() in ("danger", "high")]
        pool_addrs = {addr for addr, info in (d.get("knownAccounts") or {}).items()
                      if str((info or {}).get("type", "")).upper() in ("AMM", "POOL", "CPMM")}
        brutos = d.get("topHolders") or []
        carteiras = [h for h in brutos if h.get("owner") not in pool_addrs and h.get("address") not in pool_addrs]
        insiders = [h for h in carteiras if h.get("insider")]
        tem_mercado = bool(d.get("markets"))
        lp_pct = 0
        for m in (d.get("markets") or []):
            lp_pct = max(lp_pct, (m.get("lp") or {}).get("lpLockedPct", 0) or 0)
        r = dict(
            mint_ok=not d.get("mintAuthority"),
            freeze_ok=not d.get("freezeAuthority"),
            rugged=bool(d.get("rugged")),
            tem_mercado=tem_mercado, lp_pct=lp_pct,
            top1=max((h.get("pct", 0) for h in carteiras[:1]), default=0),
            insider_pct=sum(h.get("pct", 0) for h in insiders),
            perigo=perigo, fonte="rugcheck")
    except Exception:
        pass
    if r is None:
        try:
            d = rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])["value"]["data"]["parsed"]["info"]
            sup = float(rpc("getTokenSupply", [mint])["value"]["uiAmount"] or 0)
            v = [float(x["uiAmount"] or 0) for x in rpc("getTokenLargestAccounts", [mint])["value"]]
            r = dict(mint_ok=d.get("mintAuthority") is None, freeze_ok=d.get("freezeAuthority") is None,
                     rugged=False, tem_mercado=False, lp_pct=0,
                     top1=v[1] / sup * 100 if sup and len(v) > 1 else 0,
                     insider_pct=0, perigo=[], fonte="rpc (sem RugCheck — top1 pode incluir a pool)")
        except Exception:
            r = None
    seg_cache[mint] = (r, time.time())
    return r


def analisar(p):
    addr = p["baseToken"]["address"]
    liq = (p.get("liquidity") or {}).get("usd", 0)
    preco = float(p.get("priceUsd") or 0)
    ch = (p.get("priceChange") or {}).get("h1", 0) or 0
    ch6 = (p.get("priceChange") or {}).get("h6", 0) or 0
    ch24 = (p.get("priceChange") or {}).get("h24", 0) or 0
    ch5 = (p.get("priceChange") or {}).get("m5", 0) or 0
    idade = (time.time() * 1000 - (p.get("pairCreatedAt") or 0)) / 3.6e6
    base = dict(addr=addr, sym=p["baseToken"]["symbol"], liq=liq, ch1=ch, idade=round(idade, 1),
                url=p.get("url"), preco=preco, rug_url=f"https://rugcheck.xyz/tokens/{addr}")

    if liq < LIQ_MIN or preco <= 0:
        return dict(base, score=0,
                    rejeitado=f"liquidez abaixo de ${LIQ_MIN:,.0f}".replace(",", "."), razoes=[])
    if idade < IDADE_MIN_H:
        return dict(base, score=0,
                    rejeitado=f"par com menos de {IDADE_MIN_H}h de vida ({idade:.1f}h) — zona de rug", razoes=[])

    vol = (p.get("volume") or {}).get("h1", 0)
    if liq and vol / liq > VOL_LIQ_MAX:
        return dict(base, score=0,
                    rejeitado=f"volume/liquidez anormal ({vol/liq:.1f}x) — padrão de pump-and-dump", razoes=[])

    sec = seguranca(addr)
    if not sec:
        return dict(base, score=0, rejeitado="sem dados de segurança disponíveis", razoes=[])
    if not sec["mint_ok"]:
        return dict(base, score=0, rejeitado="MINT ATIVO — podem criar mais moedas a qualquer momento", razoes=[])
    if not sec["freeze_ok"]:
        return dict(base, score=0, rejeitado="conta pode ser congelada (freeze authority ativa)", razoes=[])
    if sec.get("rugged"):
        return dict(base, score=0, rejeitado="RugCheck marca este token como RUGGED", razoes=[])
    if sec["top1"] > 50:
        return dict(base, score=0,
                    rejeitado=f"uma carteira (fora da pool) detém {sec['top1']:.1f}% da oferta", razoes=[])
    if sec.get("insider_pct", 0) > 30:
        return dict(base, score=0,
                    rejeitado=f"carteiras ligadas entre si (insiders) detêm {sec['insider_pct']:.0f}% da oferta", razoes=[])
    if sec["perigo"]:
        return dict(base, score=0, rejeitado="RugCheck: " + ", ".join(sec["perigo"][:2]), razoes=[])
    if sec.get("tem_mercado") and sec.get("lp_pct", 0) < 50:
        return dict(base, score=0,
                    rejeitado=f"liquidez migrada mas NÃO bloqueada ({sec['lp_pct']:.0f}%)", razoes=[])
    if ch6 <= -20:
        return dict(base, score=0,
                    rejeitado=f"queda de {ch6:.0f}% nas últimas 6h — a subida de 1h é só repique", razoes=[])
    if ch5 > 3:
        return dict(base, score=0,
                    rejeitado=f"ainda a disparar agora ({ch5:.1f}% em 5min) — comprar aqui é comprar o pico", razoes=[])

    tx = (p.get("txns") or {}).get("h1", {})
    b, s = tx.get("buys", 0), tx.get("sells", 0)
    sc = 25 if liq >= 80000 else 18
    r = ["passou nos filtros de segurança (mint, freeze, rugged, LP, holders, insiders)"]
    if b + s >= 40 and b / max(s, 1) >= 1.2:
        sc += 15; r.append("mais compras que vendas")
    if 0.3 <= vol / liq <= 5:
        sc += 15; r.append("volume saudável face à liquidez")
    if 0 <= ch <= 80:
        sc += 15; r.append("subida controlada na última hora")
    elif ch > 150:
        sc -= 10; r.append("já subiu demais (>150% em 1h) — mais risco de entrar tarde")
    if IDADE_MIN_H <= idade <= 24:
        sc += 15; r.append(f"tem {idade:.1f}h de vida")
    if ch6 >= 0:
        sc += 10; r.append(f"tendência de 6h também positiva ({ch6:.0f}%)")
    else:
        r.append(f"6h ainda negativo ({ch6:.0f}%) — pondera com cautela")
    if ch24 > 400:
        sc -= 15; r.append(f"subida parabólica em 24h ({ch24:.0f}%) — risco de esgotamento")
    if -10 <= ch5 <= 0:
        sc += 15; r.append(f"recuo saudável nos últimos 5min ({ch5:.1f}%) — entrada no respiro, não no pico")
    elif ch5 > 0:
        sc -= 10; r.append(f"ainda em subida nos últimos 5min ({ch5:.1f}%) — entrada tardia, sem recuo confirmado")
    else:
        sc -= 5; r.append(f"recuo forte em 5min ({ch5:.1f}%) — pode ser reversão, não só respiro")
    if sec.get("tem_mercado"):
        sc += 10; r.append(f"liquidez confirmada bloqueada/queimada ({sec['lp_pct']:.0f}%)")
    else:
        r.append("ainda na bonding curve (pré-migração) — sem LP tradicional para bloquear ainda")
    if sec["top1"] > 15:
        sc -= 15; r.append(f"concentração elevada (maior holder isolado: {sec['top1']:.1f}%)")
    else:
        r.append(f"maior holder isolado: {sec['top1']:.1f}%")
    return dict(base, score=max(0, min(100, sc)), razoes=r)


def regra(p0, peak, pr, liq0, liq, parcial_feita=0):
    pnl = (pr / p0 - 1) * 100
    if pnl <= STOP:
        return "sair", f"STOP-LOSS ({pnl:.0f}%)"
    if liq < liq0 * (1 - LIQ_QUEDA / 100):
        return "sair", f"LIQUIDEZ A CAIR ({(liq/liq0*100):.0f}% do inicial)"
    pico_pct = (peak / p0 - 1) * 100
    for limiar, t in TRAIL_TIERS:
        if pico_pct >= limiar and pr <= peak * (1 - t / 100):
            return "sair", f"STOP MÓVEL (-{t}% do pico +{pico_pct:.0f}%)"
    if not parcial_feita and pnl >= ALVO:
        return "parcial", f"ALVO +{ALVO}%: realizou {int(PARCIAL_PCT*100)}% e sobe o stop"
    return None, ""


def varrer():
    addrs = set()
    for ep in ("token-profiles/latest/v1", "token-boosts/latest/v1", "token-boosts/top/v1"):
        try:
            addrs |= {x["tokenAddress"] for x in get(f"{DEX}/{ep}") if x.get("chainId") == "solana"}
        except Exception:
            pass
    pares = sorted(melhores(list(addrs)).values(),
                   key=lambda p: -(p.get("liquidity") or {}).get("usd", 0))[:40]
    res = sorted([analisar(p) for p in pares], key=lambda x: -x["score"])
    for a in res:
        if a["score"] >= MIN_SCORE:
            q("INSERT INTO sinais(addr,sym,score,t0,p0,liq0,peak) VALUES(%s,%s,%s,%s,%s,%s,%s) "
              "ON CONFLICT DO NOTHING",
              (a["addr"], a["sym"], a["score"], time.time(), a["preco"], a["liq"], a["preco"]))
    estado["cands"] = [a for a in res if a["score"] > 0]
    estado["rejeitados"] = [a for a in res if a["score"] == 0]
    estado["atualizado"] = time.time()
    novos = {a["addr"] for a in res if a["score"] >= MIN_SCORE}
    estado["confirmados"] = novos & estado.get("vistos_antes", set())
    estado["vistos_antes"] = novos


def hoje():
    return time.strftime("%Y-%m-%d")


def pnl_hoje():
    r = q("SELECT SUM(pnl_usd) FROM auto WHERE ativa=0 AND dia=%s", (hoje(),))
    return r[0][0] or 0


def disjuntor_ativo():
    return pnl_hoje() <= -LIMITE_PERDA_DIA_USD


def saldo_atual():
    fechado = q("SELECT SUM(pnl_usd) FROM auto WHERE ativa=0")[0][0] or 0
    aberto = 0
    for id_, addr, p0, qtd, liq0, peak, gas, custo_pct, parcial, pnl_parc, custo_ent in q(
        "SELECT id,addr,p0,qtd,liq0,peak,gas_usd,custo_pct,parcial_feita,pnl_parcial,custo_entrada_usd "
        "FROM auto WHERE ativa=1"):
        pr, _ = ultimo.get(addr, (p0, 0))
        aberto += qtd * pr + (pnl_parc or 0) - custo_ent - gas - custo_ent * custo_pct / 100
    return CAPITAL_USD + fechado + aberto


def snapshot_curva():
    q("INSERT INTO curva(ts,saldo) VALUES(%s,%s)", (time.time(), saldo_atual()))
    q("DELETE FROM curva WHERE id NOT IN (SELECT id FROM curva ORDER BY id DESC LIMIT 2000)")


def abrir_auto():
    if disjuntor_ativo():
        return
    if perdas["n"] >= COOLDOWN_PERDAS and time.time() - perdas["ts"] < COOLDOWN_MIN * 60:
        return
    abertas = q("SELECT COUNT(*) FROM auto WHERE ativa=1")[0][0]
    if abertas >= MAX_AUTO_SIMULTANEAS:
        return
    ja_abertos = {r[0] for r in q("SELECT addr FROM auto WHERE ativa=1")}
    for c in estado["cands"]:
        if abertas >= MAX_AUTO_SIMULTANEAS:
            break
        if c["score"] < MIN_SCORE:
            continue
        if c["addr"] not in estado.get("confirmados", set()):
            continue
        if c["addr"] in ja_abertos:
            continue
        if q("SELECT id FROM auto WHERE addr=%s AND dia=%s", (c["addr"], hoje())):
            continue
        p_now = melhores([c["addr"]]).get(c["addr"])
        if not p_now:
            continue
        pr_now = float(p_now.get("priceUsd") or 0)
        if pr_now <= 0:
            continue
        if abs(pr_now / c["preco"] - 1) > 0.05:
            continue
        qtd, gas = POSICAO_USD / pr_now, gas_usd_ida_volta()
        ida = custo_swap_pct_ida(c["addr"])
        custo_pct = (ida * 2) if ida is not None else 1.0
        q("INSERT INTO auto(addr,sym,p0,qtd,liq0,peak,t0,dia,gas_usd,custo_pct,custo_entrada_usd) "
          "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
          (c["addr"], c["sym"], pr_now, qtd, c["liq"], pr_now, time.time(), hoje(), gas, custo_pct, POSICAO_USD))
        ja_abertos.add(c["addr"])
        abertas += 1
        print(f"[auto] abriu {c['sym']} @ {pr_now} (gas ${gas:.4f}, swap {custo_pct:.2f}%)")


AUSENCIA_RUG = 5
SEC_RECHECK_SEG = 60


def atualizar():
    sin = q("SELECT id,addr,p0,liq0,peak,t0,parcial_feita FROM sinais WHERE estado='aberto'")
    pos = q("SELECT id,addr,peak FROM posicoes WHERE ativa=1")
    auto = q("SELECT id,addr,p0,qtd,liq0,peak,t0,gas_usd,custo_pct,parcial_feita,pnl_parcial,custo_entrada_usd "
             "FROM auto WHERE ativa=1")
    m = melhores(list({r[1] for r in sin} | {r[1] for r in pos} | {r[1] for r in auto}))

    for id_, addr, p0, qtd, liq0, peak, t0, gas, custo_pct, parcial, pnl_parc, custo_ent in auto:
        key_sec = ("autosec", addr)
        if time.time() - ausencias.get(key_sec, 0) > SEC_RECHECK_SEG:
            ausencias[key_sec] = time.time()
            seg_cache.pop(addr, None)
            s = seguranca(addr)
            if s and (not s.get("mint_ok") or not s.get("freeze_ok") or s.get("rugged")):
                pnl_final = (pnl_parc or 0) - custo_ent - gas - custo_ent * custo_pct / 100
                q("UPDATE auto SET ativa=0,pnl_usd=%s,motivo='SEGURANÇA VIOLADA EM POSIÇÃO',dia=%s WHERE id=%s",
                  (pnl_final, hoje(), id_))
                perdas["n"] += 1
                perdas["ts"] = time.time()
                print(f"[auto] fechou {addr} SEGURANÇA VIOLADA pnl={pnl_final:.3f}")
                continue

        p = m.get(addr)
        fim = time.time() - t0 > TIMEOUT
        if not p:
            ausencias[("auto", addr)] = ausencias.get(("auto", addr), 0) + 1
            if fim or ausencias[("auto", addr)] >= AUSENCIA_RUG:
                pnl_final = (pnl_parc or 0) - custo_ent - gas - custo_ent * custo_pct / 100
                q("UPDATE auto SET ativa=0,pnl_usd=%s,motivo='PAR DESAPARECEU (provável rug)',dia=%s WHERE id=%s",
                  (pnl_final, hoje(), id_))
                perdas["n"] += 1
                perdas["ts"] = time.time()
                print(f"[auto] fechou {addr} PAR DESAPARECEU pnl={pnl_final:.3f}")
            continue
        ausencias.pop(("auto", addr), None)
        pr, liq = float(p.get("priceUsd") or 0), (p.get("liquidity") or {}).get("usd", 0)
        if pr <= 0:
            continue
        peak = max(peak, pr)
        ultimo[addr] = (pr, liq)
        nivel, msg = regra(p0, peak, pr, liq0, liq, parcial)
        if nivel == "sair" or fim:
            pnl_final = (pnl_parc or 0) + qtd * pr - custo_ent - gas - custo_ent * custo_pct / 100
            q("UPDATE auto SET ativa=0,pnl_usd=%s,motivo=%s,dia=%s WHERE id=%s",
              (pnl_final, msg or "TEMPO ESGOTADO", hoje(), id_))
            if pnl_final < 0:
                perdas["n"] += 1
                perdas["ts"] = time.time()
            else:
                perdas["n"] = 0
            print(f"[auto] fechou {addr} {msg or 'TEMPO'} pnl={pnl_final:.3f}")
        elif nivel == "parcial":
            vendido = qtd * PARCIAL_PCT
            realiz = vendido * pr - custo_ent * PARCIAL_PCT
            q("UPDATE auto SET parcial_feita=1, pnl_parcial=COALESCE(pnl_parcial,0)+%s, "
              "qtd=%s, custo_entrada_usd=%s, peak=%s WHERE id=%s",
              (realiz, qtd * (1 - PARCIAL_PCT), custo_ent * (1 - PARCIAL_PCT), peak, id_))
            print(f"[auto] parcial {addr} realizou ${realiz:.3f}")
        else:
            q("UPDATE auto SET peak=%s WHERE id=%s", (peak, id_))

    abrir_auto()
    snapshot_curva()

    for id_, addr, p0, liq0, peak, t0, parcial in sin:
        p = m.get(addr)
        fim = time.time() - t0 > TIMEOUT
        if not p:
            ausencias[("sin", addr)] = ausencias.get(("sin", addr), 0) + 1
            if fim or ausencias[("sin", addr)] >= AUSENCIA_RUG:
                q("UPDATE sinais SET estado='fechado',pnl=-100,"
                  "motivo='PAR DESAPARECEU (provável rug)' WHERE id=%s", (id_,))
            continue
        ausencias.pop(("sin", addr), None)
        pr, liq = float(p.get("priceUsd") or 0), (p.get("liquidity") or {}).get("usd", 0)
        if pr <= 0:
            continue
        peak = max(peak, pr)
        nivel, msg = regra(p0, peak, pr, liq0, liq, parcial)
        if nivel == "sair" or fim:
            q("UPDATE sinais SET estado='fechado',pnl=%s,motivo=%s WHERE id=%s",
              ((pr / p0 - 1) * 100 - custo_pct_total(), msg or "TEMPO ESGOTADO", id_))
        elif nivel == "parcial":
            q("UPDATE sinais SET parcial_feita=1 WHERE id=%s", (id_,))
        else:
            q("UPDATE sinais SET peak=%s WHERE id=%s", (peak, id_))

    for id_, addr, peak in pos:
        p = m.get(addr)
        if p:
            pr = float(p.get("priceUsd") or 0)
            ultimo[addr] = (pr, (p.get("liquidity") or {}).get("usd", 0))
            q("UPDATE posicoes SET peak=%s WHERE id=%s", (max(peak, pr), id_))


def faixa(s):
    return "70-79" if s < 80 else "80-89" if s < 90 else "90-100"


def stats():
    out = []
    for lo, hi in ((70, 79), (80, 89), (90, 100)):
        r = [x[0] for x in q("SELECT pnl FROM sinais WHERE estado='fechado' AND score BETWEEN %s AND %s", (lo, hi))]
        n = len(r)
        hit = sum(1 for x in r if x > 0) / n * 100 if n else 0
        exp = sum(r) / n if n else 0
        out.append(dict(faixa=f"{lo}-{hi}", n=n, acerto=round(hit, 1), exp=round(exp, 1),
                        apto=n >= MIN_AMOSTRAS and hit >= MIN_ACERTO and exp > 0))
    return out


def loop_scan():
    while True:
        try:
            varrer()
            estado["erro"] = ""
        except Exception as e:
            estado["erro"] = f"Este varrimento falhou ({str(e)[:120]}). A mostrar o último resultado válido."
            print("[loop_scan]", e)
        time.sleep(INTERVALO_SCAN)


def loop_pos():
    while True:
        try:
            atualizar()
        except Exception as e:
            print("[loop_pos]", e)
        time.sleep(INTERVALO_POS)


@app.route("/")
def home():
    return send_from_directory("static", "index.html")


@app.route("/api/estado")
def api():
    pos = []
    for id_, addr, sym, p0, liq0, peak in q("SELECT id,addr,sym,p0,liq0,peak FROM posicoes WHERE ativa=1"):
        pr, liq = ultimo.get(addr, (p0, liq0))
        nivel, msg = regra(p0, peak, pr, liq0, liq)
        pos.append(dict(id=id_, sym=sym, p0=p0, preco=pr,
                        pnl=round((pr / p0 - 1) * 100, 1), nivel=nivel, msg=msg))

    st = stats()
    apto = {f["faixa"]: f["apto"] for f in st}

    cands_saida = []
    for c in estado["cands"]:
        c2 = dict(c)
        c2["elegivel"] = c2["score"] >= MIN_SCORE
        c2["apto"] = c2["elegivel"] and apto.get(faixa(c2["score"]), False)
        c2["confirmado"] = c2["addr"] in estado.get("confirmados", set())
        cands_saida.append(c2)

    sig = [dict(sym=s, score=sc, estado=e, pnl=None if p is None else round(p, 1), motivo=m)
           for s, sc, e, p, m in q("SELECT sym,score,estado,pnl,motivo FROM sinais ORDER BY id DESC LIMIT 12")]

    a_abertas = []
    for id_, addr, sym, p0, qtd, peak, gas, custo_pct, parcial, pnl_parc, custo_ent in q(
        "SELECT id,addr,sym,p0,qtd,peak,gas_usd,custo_pct,parcial_feita,pnl_parcial,custo_entrada_usd "
        "FROM auto WHERE ativa=1"):
        pr, liq = ultimo.get(addr, (p0, 0))
        pnl_usd = qtd * pr + (pnl_parc or 0) - custo_ent - gas - custo_ent * custo_pct / 100
        a_abertas.append(dict(sym=sym, p0=p0, preco=pr, valor=round(qtd * pr, 2),
                              gas=round(gas, 3), swap=round(custo_ent * custo_pct / 100, 3),
                              parcial=bool(parcial), pnl_usd=round(pnl_usd, 2)))

    a_hist = [dict(sym=s, pnl_usd=round(pl, 2), motivo=m,
                   gas=round(g or 0, 3), swap=round(custo_ent * (cp or 0) / 100, 3))
              for s, pl, m, g, cp, custo_ent in
              q("SELECT sym,pnl_usd,motivo,gas_usd,custo_pct,custo_entrada_usd "
                "FROM auto WHERE ativa=0 ORDER BY id DESC LIMIT 8")]

    curva = [dict(ts=ts, saldo=round(s, 2))
             for ts, s in q("SELECT ts,saldo FROM curva ORDER BY id ASC LIMIT 2000")]

    return jsonify(
        cands=cands_saida[:15],
        rejeitados=estado["rejeitados"][:15],
        pos=pos, stats=st, sinais=sig,
        auto=dict(capital=CAPITAL_USD, tamanho=POSICAO_USD, limite_dia=LIMITE_PERDA_DIA_USD,
                  max_simultaneas=MAX_AUTO_SIMULTANEAS, pnl_hoje=round(pnl_hoje(), 2),
                  disjuntor=disjuntor_ativo(), abertas=a_abertas, historico=a_hist,
                  saldo=round(saldo_atual(), 2), curva=curva,
                  gas_agora=round(gas_usd_ida_volta(), 3),
                  perdas_consecutivas=perdas["n"]),
        atualizado=estado["atualizado"], erro=estado["erro"])


@app.route("/api/posicao", methods=["POST"])
def nova():
    d = request.json
    addr = d["addr"].strip()
    p = melhores([addr]).get(addr)
    if not p:
        return jsonify(erro="Token não encontrado na Solana"), 404
    pr, liq = float(p["priceUsd"]), p["liquidity"]["usd"]
    p0 = float(d.get("p0") or pr)
    q("INSERT INTO posicoes(addr,sym,p0,liq0,peak,t0) VALUES(%s,%s,%s,%s,%s,%s)",
      (addr, p["baseToken"]["symbol"], p0, liq, max(p0, pr), time.time()))
    ultimo[addr] = (pr, liq)
    return jsonify(ok=1)


@app.route("/api/posicao/<int:i>/fechar", methods=["POST"])
def fechar(i):
    q("UPDATE posicoes SET ativa=0 WHERE id=%s", (i,))
    return jsonify(ok=1)


# ------------------------------------------------------------------
# Arranque dos loops em nível de módulo (gunicorn NÃO executa __main__)
# ------------------------------------------------------------------
_loops_arrancados = False
def _arrancar_loops():
    global _loops_arrancados
    if _loops_arrancados:
        return
    _loops_arrancados = True
    threading.Thread(target=loop_scan, daemon=True).start()
    threading.Thread(target=loop_pos, daemon=True).start()
    print("[arranque] loops scan + pos iniciados")

_arrancar_loops()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)