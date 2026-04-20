"""
Збір market-level даних з 10 perpetual DEX бірж + категоризація.

Запускається через GitHub Actions (fetch.yml) вручну.
Результат: 10 parquet-файлів у data/ з 13 уніфікованими колонками кожен.

Архітектура:
  Фаза 1 — швидкий збір (~30 сек): HL native + 8 HIP-3, Aster, Lighter,
           Paradex, Variational, Extended, Pacifica
  Фаза 2 — per-instrument (~5 хв): GRVT, EdgeX, ApeX
  Фаза 3 — категоризація (~5 сек): categorize.apply() для кожного DataFrame

Verbose логи — усі кроки друкуються у stdout, видно у GitHub Actions logs.

Фіксує 13 грабельок з playbook v4:
  #1  User-Agent обовʼязковий (6 з 10 бірж без нього віддають 503)
  #2  Транзієнтний 503 "DNS cache overflow" — retry до 8 разів
  #3  r.json() крах на не-JSON відповіді — перевірка r.text[0] in '{['
  #4  Paradex 596 ≠ 84 PERP — фільтр asset_kind="PERP"
  #5  ApeX /v3/ticker без symbol віддає [] — per-instrument запити
  #6  ApeX 3 типи контрактів — тільки perpetualContract
  #7  GRVT volume_24h_q НЕ існує — треба buy+sell
  #8  EdgeX getTicketSummary не працює — getTicker per contractId
  #9  Pacifica /api/v1/tickers → 404 — треба /info/prices
  #10 Різні стани активності пар
  #11 EdgeX/ApeX не мають baseCoinName — треба coin_map / underlyingCurrencyId
  #12 Lighter USD-суфікс без сепаратора — обробляє canonical_base
  #13 Hyperliquid HIP-3 — 8 окремих dex'ів (+135 пар, +$1.67B)
"""
import os
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

import categorize

# ---------------------------------------------------------------------------
# Конфіг
# ---------------------------------------------------------------------------

DEBUG = True  # Verbose логи у stdout (видно у GitHub Actions)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36"
}
S = requests.Session()
S.headers.update(HEADERS)

os.makedirs("data", exist_ok=True)

# ---------------------------------------------------------------------------
# HTTP-обгортки з retry (грабельки #1–3)
# ---------------------------------------------------------------------------

def _get(url, **kw):
    """
    GET з retry до 8 разів, перевіркою status 200 і що тіло — JSON.

    Обробка 403: ApeX та EdgeX повертають 403 при rate limit
    (не стандартне 429). При 403 — довша пауза 5 сек.
    """
    r = None
    for attempt in range(8):
        try:
            r = S.get(url, timeout=25, **kw)
            if r.status_code == 200 and r.text and r.text[0] in "{[":
                return r
            # 403 = rate limit ban у ApeX/EdgeX — довша пауза
            if r.status_code == 403:
                time.sleep(5)
            else:
                time.sleep(2)
        except Exception:
            time.sleep(2)
    return r


def _post(url, **kw):
    """POST з retry до 8 разів з такою ж 403-обробкою."""
    r = None
    for attempt in range(8):
        try:
            r = S.post(url, timeout=25, **kw)
            if r.status_code == 200 and r.text and r.text[0] in "{[":
                return r
            if r.status_code == 403:
                time.sleep(5)
            else:
                time.sleep(2)
        except Exception:
            time.sleep(2)
    return r


def jget(url, **kw):
    """GET + r.json() з retry."""
    for _ in range(8):
        r = _get(url, **kw)
        try:
            return r.json()
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"Failed JSON from: {url}")


def jpost(url, **kw):
    """POST + r.json() з retry."""
    for _ in range(8):
        r = _post(url, **kw)
        try:
            return r.json()
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"Failed JSON from: {url}")


def log(msg: str) -> None:
    """Verbose print (можна вимкнути через DEBUG=False)."""
    if DEBUG:
        print(msg, flush=True)


# ===========================================================================
# 1. Hyperliquid — native + 8 HIP-3 dex'ів (грабелька #13)
# ===========================================================================

def fetch_hyperliquid():
    url = "https://api.hyperliquid.xyz/info"
    all_rows = []

    # --- 1a) Native dex ---
    log("🔵 Hyperliquid native: збираю...")
    t0 = time.time()
    data = jpost(url, json={"type": "metaAndAssetCtxs"})
    meta, ctxs = data[0], data[1]
    for u, c in zip(meta["universe"], ctxs):
        if u.get("isDelisted"):
            continue
        name = u["name"]
        all_rows.append({
            "Pair":             f"{name}/USDC",
            "Price (USDC)":     float(c.get("markPx") or 0),
            "Volume 24h (USD)": float(c.get("dayNtlVlm") or 0),
            "Open Interest":    float(c.get("openInterest") or 0),
            "Funding Rate":     float(c.get("funding") or 0),
            "_hip3_dex":        None,
        })
    native_count = len(all_rows)
    native_vol = sum(r["Volume 24h (USD)"] for r in all_rows)
    log(f"✅ Hyperliquid native: {native_count} pairs, ${native_vol:,.0f} ({time.time()-t0:.1f}s)")

    # --- 1b) HIP-3 dex'и ---
    log("🔵 Hyperliquid HIP-3: шукаю dex'и...")
    dexs_info = jpost(url, json={"type": "perpDexs"})
    hip3_names = [d["name"] for d in dexs_info if d is not None]
    log(f"   → знайдено {len(hip3_names)} HIP-3 dex'ів: {', '.join(hip3_names)}")

    for dex in hip3_names:
        t1 = time.time()
        try:
            d = jpost(url, json={"type": "metaAndAssetCtxs", "dex": dex})
            universe, ctxs = d[0]["universe"], d[1]
            dex_rows = []
            for u, c in zip(universe, ctxs):
                if u.get("isDelisted"):
                    continue
                base = u["name"]
                dex_rows.append({
                    "Pair":             f"{dex}:{base}/USDC",
                    "Price (USDC)":     float(c.get("markPx") or 0),
                    "Volume 24h (USD)": float(c.get("dayNtlVlm") or 0),
                    "Open Interest":    float(c.get("openInterest") or 0),
                    "Funding Rate":     float(c.get("funding") or 0),
                    "_hip3_dex":        dex,
                })
            dex_vol = sum(r["Volume 24h (USD)"] for r in dex_rows)
            all_rows.extend(dex_rows)
            log(f"✅ HIP-3 {dex:8s}: {len(dex_rows):>3} pairs, ${dex_vol:>15,.0f} ({time.time()-t1:.1f}s)")
        except Exception as e:
            log(f"❌ HIP-3 {dex}: {type(e).__name__}: {e}")
        time.sleep(0.5)

    df = pd.DataFrame(all_rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    hip3_count = len(df) - native_count
    hip3_vol = df["Volume 24h (USD)"].sum() - native_vol
    log(f"📊 Hyperliquid TOTAL: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} "
        f"(native {native_count} + HIP-3 {hip3_count} / +${hip3_vol:,.0f})")

    # Категоризація (base_hint з канонізатора, HIP-3 префікс вже у Pair)
    df = categorize.apply(df, "Hyperliquid")

    # Fill HIP-3 Dex з власного поля (не з canonical_base — щоб зберегти native=None)
    df["HIP-3 Dex"] = df["_hip3_dex"]
    df = df.drop(columns=["_hip3_dex"])

    df.to_parquet("data/hl_cache.parquet", index=False)
    return df


# ===========================================================================
# 2. Aster DEX — фільтр contractType=PERPETUAL + status=TRADING (грабелька #10)
# ===========================================================================

def fetch_aster():
    log("🔵 Aster DEX: збираю...")
    t0 = time.time()

    # exchangeInfo для фільтру
    ei = jget("https://fapi.asterdex.com/fapi/v1/exchangeInfo")
    perp_syms = {
        s["symbol"] for s in ei.get("symbols", [])
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
    }
    log(f"   → відфільтровано {len(perp_syms)} PERP з {len(ei.get('symbols', []))} всього")

    # 24h tickers для volumes
    tk = jget("https://fapi.asterdex.com/fapi/v1/ticker/24hr")
    rows = []
    for t in tk:
        sym = t.get("symbol", "")
        if sym not in perp_syms:
            continue
        rows.append({
            "Pair":             sym,
            "Price (USDT)":     float(t.get("lastPrice") or 0),
            "Change 24h (%)":   float(t.get("priceChangePercent") or 0),
            "Volume 24h (USD)": float(t.get("quoteVolume") or 0),
            "High 24h":         float(t.get("highPrice") or 0),
            "Low 24h":          float(t.get("lowPrice") or 0),
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ Aster DEX: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} ({time.time()-t0:.1f}s)")

    df = categorize.apply(df, "Aster DEX")
    df.to_parquet("data/aster_cache.parquet", index=False)
    return df


# ===========================================================================
# 3. Lighter — exchangeStats (USD-суфікс обробляє canonical_base, грабелька #12)
# ===========================================================================

def fetch_lighter():
    log("🔵 Lighter: збираю...")
    t0 = time.time()
    r = jget("https://mainnet.zklighter.elliot.ai/api/v1/exchangeStats")
    rows = [{
        "Pair":             s.get("symbol", ""),  # bare symbol без /USD
        "Price":            float(s.get("last_trade_price") or 0),
        "Volume 24h (USD)": float(s.get("daily_quote_token_volume") or 0),
        "Change 24h (%)":   float(s.get("daily_price_change") or 0),
        "Trades 24h":       int(s.get("daily_trades_count") or 0),
    } for s in r.get("order_book_stats", [])]
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ Lighter: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} ({time.time()-t0:.1f}s)")

    df = categorize.apply(df, "Lighter")
    df.to_parquet("data/lighter_cache.parquet", index=False)
    return df


# ===========================================================================
# 4. Paradex — фільтр asset_kind=PERP (грабелька #4)
# ===========================================================================

def fetch_paradex():
    log("🔵 Paradex: збираю...")
    t0 = time.time()

    # Markets list — фільтр asset_kind
    markets = jget("https://api.prod.paradex.trade/v1/markets").get("results", [])
    perp_symbols = {m["symbol"] for m in markets if m.get("asset_kind") == "PERP"}
    log(f"   → відфільтровано {len(perp_symbols)} PERP з {len(markets)} "
        f"(опціонів/spot: {len(markets) - len(perp_symbols)})")

    # Volumes — окремий endpoint
    summary = jget("https://api.prod.paradex.trade/v1/markets/summary?market=ALL").get("results", [])
    rows = []
    for m in summary:
        sym = m.get("symbol", "")
        if sym not in perp_symbols:
            continue
        rows.append({
            "Pair":             sym,
            "Price (USDC)":     float(m.get("mark_price") or 0),
            "Volume 24h (USD)": float(m.get("volume_24h") or 0),
            "Open Interest":    float(m.get("open_interest") or 0),
            "Funding Rate":     float(m.get("funding_rate") or 0) if m.get("funding_rate") else 0,
            "Change 24h (%)":   float(m.get("price_change_rate_24h") or 0) * 100,
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ Paradex: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} ({time.time()-t0:.1f}s)")

    df = categorize.apply(df, "Paradex")
    df.to_parquet("data/paradex_cache.parquet", index=False)
    return df


# ===========================================================================
# 5. Variational Omni — /metadata/stats (один запит — все)
# ===========================================================================

def fetch_variational():
    log("🔵 Variational Omni: збираю...")
    t0 = time.time()
    r = jget("https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats")
    rows = []
    for l in r.get("listings", []):
        oi = l.get("open_interest", {})
        long_oi = float(oi.get("long_open_interest") or 0)
        short_oi = float(oi.get("short_open_interest") or 0)
        rows.append({
            "Pair":             l["ticker"] + "/USDC",
            "Price (USDC)":     float(l.get("mark_price") or 0),
            "Volume 24h (USD)": float(l.get("volume_24h") or 0),
            "Open Interest":    long_oi + short_oi,
            "Funding Rate":     float(l.get("funding_rate") or 0),
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ Variational Omni: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} ({time.time()-t0:.1f}s)")

    df = categorize.apply(df, "Variational Omni")
    df.to_parquet("data/variational_cache.parquet", index=False)
    return df


# ===========================================================================
# 6. Extended — фільтр ACTIVE + REDUCE_ONLY, з category hint (грабелька #10)
# ===========================================================================

def fetch_extended():
    log("🔵 Extended: збираю...")
    t0 = time.time()
    r = jget("https://api.starknet.extended.exchange/api/v1/info/markets")
    rows = []
    for m in r.get("data", []):
        if m.get("status") not in ("ACTIVE", "REDUCE_ONLY"):
            continue
        s = m.get("marketStats", {})
        rows.append({
            "Pair":             m.get("name", ""),
            "Price (USD)":      float(s.get("lastPrice") or 0),
            "Volume 24h (USD)": float(s.get("dailyVolume") or 0),
            "Open Interest":    float(s.get("openInterest") or 0),
            "Funding Rate":     float(s.get("fundingRate") or 0),
            "Change 24h (%)":   float(s.get("dailyPriceChangePercentage") or 0) * 100,
            "_category_hint":   m.get("category", ""),  # Extended віддає category!
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ Extended: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} ({time.time()-t0:.1f}s)")

    # Передаємо category hint у categorize
    df = categorize.apply(df, "Extended", category_hint_col="_category_hint")
    df = df.drop(columns=["_category_hint"])
    df.to_parquet("data/extended_cache.parquet", index=False)
    return df


# ===========================================================================
# 7. Pacifica — два endpoints (грабелька #9)
# ===========================================================================

def fetch_pacifica():
    log("🔵 Pacifica: збираю...")
    t0 = time.time()
    r = jget("https://api.pacifica.fi/api/v1/info/prices")
    rows = []
    for p in r.get("data", []):
        mark = float(p.get("mark") or 0)
        yest = float(p.get("yesterday_price") or 0) or mark
        change_pct = ((mark - yest) / yest * 100) if yest else 0
        rows.append({
            "Pair":             p.get("symbol", "") + "/USD",
            "Price (USD)":      mark,
            "Volume 24h (USD)": float(p.get("volume_24h") or 0),
            "Open Interest":    float(p.get("open_interest") or 0),
            "Funding Rate":     float(p.get("funding") or 0),
            "Change 24h (%)":   change_pct,
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ Pacifica: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} ({time.time()-t0:.1f}s)")

    df = categorize.apply(df, "Pacifica")
    df.to_parquet("data/pacifica_cache.parquet", index=False)
    return df


# ===========================================================================
# 8. GRVT — per-instrument, buy+sell volumes (грабельки #7, #10)
# ===========================================================================

def _grvt_one(instrument):
    try:
        r = _post(
            "https://market-data.grvt.io/full/v1/ticker",
            json={"instrument": instrument},
        )
        if r.status_code != 200:
            return None
        d = r.json().get("result", {})
        if not d:
            return None
        buy_q = float(d.get("buy_volume_24h_q") or 0)
        sell_q = float(d.get("sell_volume_24h_q") or 0)
        last = float(d.get("last_price") or 0)
        opn = float(d.get("open_price") or 0) or last
        change_pct = ((last - opn) / opn * 100) if opn else 0
        return {
            "Pair":             instrument,
            "Price (USDT)":     last,
            "Volume 24h (USD)": buy_q + sell_q,  # грабелька #7
            "Open Interest":    float(d.get("open_interest") or 0),
            "Funding Rate":     float(d.get("funding_rate") or 0),
            "Change 24h (%)":   change_pct,
            "High 24h":         float(d.get("high_price") or 0),
            "Low 24h":          float(d.get("low_price") or 0),
        }
    except Exception:
        return None


def fetch_grvt():
    log("🔵 GRVT: збираю список інструментів...")
    t0 = time.time()
    r = jpost("https://market-data.grvt.io/full/v1/all_instruments", json={"is_active": True})
    instruments = [
        i["instrument"] for i in r.get("result", [])
        if i.get("kind") == "PERPETUAL"
    ]
    log(f"   → {len(instruments)} PERP instruments, запитую ticker для кожного...")

    rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for i, res in enumerate(ex.map(_grvt_one, instruments)):
            if res:
                rows.append(res)
            if i % 25 == 0 and i > 0:
                time.sleep(0.5)

    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ GRVT: {len(df)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} ({time.time()-t0:.1f}s)")

    df = categorize.apply(df, "GRVT")
    df.to_parquet("data/grvt_cache.parquet", index=False)
    return df


# ===========================================================================
# 9. EdgeX — per-contract, з coin_map для базового токена (грабельки #8, #11)
#
# 🆕 Rate limit (грабелька #14): EdgeX не публікує офіційні ліміти, але з
# емпіричних даних (лог 2026-04-20: 94 з 186 пар загубилось при 15 воркерах)
# зʼясовано що паралельні запити призводять до 403/тихих failures.
# Рішення: SEQUENTIAL виконання з паузою 0.2с між запитами = ~5 req/sec.
# Час: 186 × 0.2 = ~37с (раніше було 120с з retry через 503).
# ===========================================================================

EDGEX_DELAY = 0.2  # секунд між послідовними запитами (~5 req/sec)


def _edgex_one(c: dict, coin_map: dict) -> tuple[dict | None, str | None]:
    """
    Повертає (result, error_reason).
    error_reason = None при успіху, інакше — короткий опис причини відмови.
    """
    cid = c.get("contractId")
    contract_name = c.get("contractName", "?")
    try:
        r = _get(f"https://pro.edgex.exchange/api/v1/public/quote/getTicker?contractId={cid}")

        if r is None:
            return None, f"{contract_name}: no response"
        if r.status_code == 403:
            return None, f"{contract_name}: 403 rate limit ban"
        if r.status_code != 200:
            return None, f"{contract_name}: HTTP {r.status_code}"

        try:
            payload = r.json()
        except Exception:
            return None, f"{contract_name}: invalid JSON"

        # Грабелька: EdgeX використовує code='SUCCESS' як маркер успіху
        if payload.get("code") != "SUCCESS":
            return None, f"{contract_name}: code={payload.get('code')}"

        data = payload.get("data", [])
        if not data:
            return None, f"{contract_name}: empty data (new/inactive contract)"

        t = data[0]
        base = coin_map.get(c.get("baseCoinId"), "")
        return {
            "Pair":             contract_name,
            "Base":             base,
            "Price (USD)":      float(t.get("lastPrice") or 0),
            "Volume 24h (USD)": float(t.get("value") or 0),
            "Open Interest":    float(t.get("openInterest") or 0),
            "Funding Rate":     float(t.get("fundingRate") or 0),
            "Change 24h (%)":   float(t.get("priceChangePercent") or 0) * 100,
            "Trades 24h":       int(float(t.get("trades") or 0)),
            "High 24h":         float(t.get("high") or 0),
            "Low 24h":          float(t.get("low") or 0),
        }, None
    except Exception as e:
        return None, f"{contract_name}: {type(e).__name__}: {e}"


def fetch_edgex():
    log("🔵 EdgeX: збираю metadata...")
    t0 = time.time()
    meta = jget("https://pro.edgex.exchange/api/v1/public/meta/getMetaData")

    # coin_map: baseCoinId → coinName (грабелька #11)
    coin_map = {c["coinId"]: c["coinName"] for c in meta["data"].get("coinList", [])}
    contracts = meta["data"].get("contractList", [])

    # Фільтр enableTrade (грабелька #10)
    active = [c for c in contracts if c.get("enableTrade") in (True, "true", None)]
    disabled = len(contracts) - len(active)
    log(f"   → {len(active)} активних з {len(contracts)} ({disabled} disabled *2USD)")
    log(f"   → coin_map: {len(coin_map)} токенів")
    log(f"   → послідовні запити з паузою {EDGEX_DELAY}с (~{1/EDGEX_DELAY:.0f} req/sec)...")

    rows = []
    failures = []
    for i, c in enumerate(active):
        result, err = _edgex_one(c, coin_map)
        if result:
            rows.append(result)
        elif err:
            failures.append(err)
        time.sleep(EDGEX_DELAY)
        # Прогрес-лог кожні 50 запитів
        if (i + 1) % 50 == 0:
            log(f"   ... {i+1}/{len(active)} ({len(rows)} ok, {len(failures)} fail)")

    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ EdgeX: {len(df)}/{len(active)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} "
        f"({time.time()-t0:.1f}s)")

    # Якщо багато невдач — показати топ-5 причин для діагностики
    if failures:
        log(f"   ⚠️ {len(failures)} запитів не дали результату, приклади:")
        for f in failures[:5]:
            log(f"      • {f}")

    df = categorize.apply(df, "EdgeX", base_col="Base")
    df.to_parquet("data/edgex_cache.parquet", index=False)
    return df


# ===========================================================================
# 10. ApeX Omni — per-symbol, crossSymbolName + underlyingCurrencyId (грабельки #5, #6, #11)
#
# 🆕 Rate limit (грабелька #14): ApeX офіційно декларує 600 req / 60 сек по IP
# (= 10 req/sec). При перевищенні — HTTP 403 + тимчасовий/постійний бан IP.
# Рішення: SEQUENTIAL виконання з паузою 0.11с = ~9 req/sec (запас під ліміт).
# Час: 133 × 0.11 = ~15с (раніше 10 воркерів паралельно → 10 пар губилося).
# ===========================================================================

APEX_DELAY = 0.11  # секунд між запитами (~9 req/sec, запас під 10/sec ліміт)


def _apex_one(pc: dict) -> tuple[dict | None, str | None]:
    """
    pc = {"symbol": "BTC-USDT", "crossSymbolName": "BTCUSDT", "underlyingCurrencyId": "BTC"}
    Повертає (result, error_reason).
    """
    cs = pc.get("crossSymbolName", "")
    display = pc.get("symbol", "?")
    base = pc.get("underlyingCurrencyId") or display.split("-")[0]

    try:
        r = _get(f"https://omni.apex.exchange/api/v3/ticker?symbol={cs}")

        if r is None:
            return None, f"{display}: no response"
        if r.status_code == 403:
            return None, f"{display}: 403 rate limit ban"
        if r.status_code != 200:
            return None, f"{display}: HTTP {r.status_code}"

        try:
            payload = r.json()
        except Exception:
            return None, f"{display}: invalid JSON"

        data = payload.get("data", [])
        if not data:
            return None, f"{display}: empty data"

        t = data[0]
        return {
            "Pair":             display,
            "Base":             base,
            "Price (USDT)":     float(t.get("lastPrice") or 0),
            "Volume 24h (USD)": float(t.get("turnover24h") or 0),
            "Open Interest":    float(t.get("openInterest") or 0),
            "Funding Rate":     float(t.get("fundingRate") or 0),
            "Change 24h (%)":   float(t.get("price24hPcnt") or 0) * 100,
            "High 24h":         float(t.get("highPrice24h") or 0),
            "Low 24h":          float(t.get("lowPrice24h") or 0),
        }, None
    except Exception as e:
        return None, f"{display}: {type(e).__name__}: {e}"


def fetch_apex():
    log("🔵 ApeX Omni: збираю symbols...")
    t0 = time.time()
    r = jget("https://omni.apex.exchange/api/v3/symbols")

    # Тільки perpetualContract (грабелька #6)
    perps = r.get("data", {}).get("contractConfig", {}).get("perpetualContract", []) or []
    log(f"   → {len(perps)} PERP (відкинуто prediction/stock contracts)")
    log(f"   → послідовні запити з паузою {APEX_DELAY}с (~{1/APEX_DELAY:.0f} req/sec, "
        f"ліміт 10/sec по IP)...")

    rows = []
    failures = []
    for i, pc in enumerate(perps):
        result, err = _apex_one(pc)
        if result:
            rows.append(result)
        elif err:
            failures.append(err)
        time.sleep(APEX_DELAY)
        if (i + 1) % 50 == 0:
            log(f"   ... {i+1}/{len(perps)} ({len(rows)} ok, {len(failures)} fail)")

    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    log(f"✅ ApeX Omni: {len(df)}/{len(perps)} pairs, ${df['Volume 24h (USD)'].sum():,.0f} "
        f"({time.time()-t0:.1f}s)")

    if failures:
        log(f"   ⚠️ {len(failures)} запитів не дали результату, приклади:")
        for f in failures[:5]:
            log(f"      • {f}")

    df = categorize.apply(df, "ApeX Omni", base_col="Base")
    df.to_parquet("data/apex_cache.parquet", index=False)
    return df


# ===========================================================================
# 11. Cloudflare Radar — гео-дані (top locations) для 10 бірж
# ===========================================================================
# Endpoint: /radar/ranking/domain/{domain}?includeTopLocations=true
# Повертає ранг домену + масив топ-країн з %-часткою трафіку.
# Працює тільки для доменів, які у Cloudflare ranking (~top-1M за DNS-трафіком 1.1.1.1).
# Для доменів без даних — пропускаємо (API поверне 404 або порожній topLocations).

RADAR_DOMAINS = {
    "Hyperliquid":       "hyperliquid.xyz",
    "Aster DEX":         "asterdex.com",
    "Lighter":           "app.lighter.xyz",
    "Paradex":           "paradex.trade",
    "Variational Omni":  "variational.io",
    "Extended":          "extended.exchange",
    "Pacifica":          "pacifica.fi",
    "GRVT":              "grvt.io",
    "EdgeX":             "pro.edgex.exchange",
    "ApeX Omni":         "apex.exchange",
}

def _flag_emoji(alpha2: str) -> str:
    """Конвертує ISO alpha-2 ('US') у прапор-емоджі ('🇺🇸')."""
    if not alpha2 or len(alpha2) != 2:
        return ""
    return "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in alpha2.upper())

def fetch_cloudflare_radar():
    token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if not token:
        raise RuntimeError("CLOUDFLARE_API_TOKEN не встановлено у environment")

    headers = {"Authorization": f"Bearer {token}"}
    rows = []
    fetched_at = pd.Timestamp.utcnow().isoformat()

    for exchange, domain in RADAR_DOMAINS.items():
        try:
            r = requests.get(
                f"https://api.cloudflare.com/client/v4/radar/ranking/domain/{domain}",
                headers=headers,
                params={"includeTopLocations": "true", "limit": 100},
                timeout=15,
            )
            if r.status_code != 200:
                print(f"  ⚠️  {exchange} ({domain}): HTTP {r.status_code}")
                continue

            result = r.json().get("result", {})
            details = result.get("details_0", {})
            top_locations = details.get("topLocations", [])

            if not top_locations:
                print(f"  ⚠️  {exchange} ({domain}): no top locations")
                continue

            for loc in top_locations:
                alpha2 = loc.get("locationCode", "")
                rows.append({
                    "Exchange":     exchange,
                    "Domain":       domain,
                    "CountryCode":  alpha2,
                    "CountryName":  loc.get("locationName", ""),
                    "Share":        float(loc.get("value", 0)),
                    "Rank":         loc.get("rank", 0) if loc.get("rank") else 0,
                    "Flag":         _flag_emoji(alpha2),
                    "FetchedAt":    fetched_at,
                })
            print(f"  ✓ {exchange} ({domain}): {len(top_locations)} countries")

        except Exception as e:
            print(f"  ✗ {exchange} ({domain}): {type(e).__name__}: {e}")

    if not rows:
        print("  ⚠️  no radar data — parquet not created")
        return

    df = pd.DataFrame(rows)
    df.to_parquet("data/radar_cache.parquet", index=False)
    print(f"Cloudflare Radar: {len(df)} rows saved")

# ===========================================================================
# Запуск — кожна біржа в try/except, щоб одна помилка не зламала весь fetch
# ===========================================================================

ALL_FETCHERS = [
    ("Hyperliquid", fetch_hyperliquid),
    ("Aster",       fetch_aster),
    ("Lighter",     fetch_lighter),
    ("Paradex",     fetch_paradex),
    ("Variational", fetch_variational),
    ("Extended",    fetch_extended),
    ("Pacifica",    fetch_pacifica),
    ("GRVT",        fetch_grvt),
    ("EdgeX",       fetch_edgex),
    ("ApeX Omni",   fetch_apex),
    ("Cloudflare Radar", fetch_cloudflare_radar),
]

if __name__ == "__main__":
    print("Починаємо збір даних з 10 бірж + Cloudflare Radar...")
    t0 = time.time()
    failed = []
    for name, fn in ALL_FETCHERS:
        t1 = time.time()
        try:
            fn()
            print(f"  ✅ {name}: {time.time()-t1:.1f}s")
        except Exception as e:
            print(f"  ❌ {name}: {type(e).__name__}: {e}")
            failed.append(name)
    print(f"\nГотово за {time.time()-t0:.1f}s. Невдач: {len(failed)} ({', '.join(failed) if failed else '—'})")
