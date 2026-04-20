"""
Збір market-level даних з 10 perpetual DEX бірж.
Запускається через GitHub Actions (workflow_dispatch).
Зберігає результати у data/*.parquet.
"""
import os
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

os.makedirs("data", exist_ok=True)

# ===========================================================================
# 1. Hyperliquid — один запит metaAndAssetCtxs
# ===========================================================================

def fetch_hyperliquid():
    r = requests.post(
        "https://api.hyperliquid.xyz/info",
        headers={"Content-Type": "application/json"},
        json={"type": "metaAndAssetCtxs"},
        timeout=15,
    )
    r.raise_for_status()
    meta, ctxs = r.json()
    rows = [{
        "Pair":             a["name"] + "/USDC",
        "Price (USDC)":     float(c.get("markPx") or 0),
        "Volume 24h (USD)": float(c.get("dayNtlVlm") or 0),
        "Open Interest":    float(c.get("openInterest") or 0),
        "Funding Rate":     float(c.get("funding") or 0),
    } for a, c in zip(meta["universe"], ctxs)]
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/hl_cache.parquet", index=False)
    print(f"Hyperliquid: {len(df)} пар збережено")

# ===========================================================================
# 2. Aster DEX — Binance-сумісний /ticker/24hr
# ===========================================================================

def fetch_aster():
    r = requests.get("https://fapi.asterdex.com/fapi/v1/ticker/24hr", timeout=15)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    for col in ["lastPrice", "priceChangePercent", "quoteVolume", "highPrice", "lowPrice"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.rename(columns={
        "symbol":             "Pair",
        "lastPrice":          "Price (USDT)",
        "priceChangePercent": "Change 24h (%)",
        "quoteVolume":        "Volume 24h (USD)",
        "highPrice":          "High 24h",
        "lowPrice":           "Low 24h",
    })
    cols = [c for c in ["Pair", "Price (USDT)", "Change 24h (%)", "Volume 24h (USD)", "High 24h", "Low 24h"] if c in df.columns]
    df = df[cols].sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/aster_cache.parquet", index=False)
    print(f"Aster DEX: {len(df)} пар збережено")

# ===========================================================================
# 3. Lighter — exchangeStats
# ===========================================================================

def fetch_lighter():
    r = requests.get("https://mainnet.zklighter.elliot.ai/api/v1/exchangeStats", timeout=15)
    r.raise_for_status()
    rows = [{
        "Pair":             s.get("symbol", "") + "/USD",
        "Price":            float(s.get("last_trade_price") or 0),
        "Volume 24h (USD)": float(s.get("daily_quote_token_volume") or 0),
        "Change 24h (%)":   float(s.get("daily_price_change") or 0),
        "Trades 24h":       int(s.get("daily_trades_count") or 0),
    } for s in r.json().get("order_book_stats", [])]
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/lighter_cache.parquet", index=False)
    print(f"Lighter: {len(df)} пар збережено")

# ===========================================================================
# 4. Paradex — markets/summary?market=ALL (фільтруємо тільки PERP, без опціонів)
# ===========================================================================

def fetch_paradex():
    r = requests.get(
        "https://api.prod.paradex.trade/v1/markets/summary",
        params={"market": "ALL"},
        timeout=20,
    )
    r.raise_for_status()
    rows = []
    for m in r.json().get("results", []):
        sym = m.get("symbol", "")
        if not sym.endswith("-PERP"):
            continue  # пропускаємо опціони (BTC-USD-29MAY26-235000-C тощо)
        rows.append({
            "Pair":             sym,
            "Price (USDC)":     float(m.get("mark_price") or 0),
            "Volume 24h (USD)": float(m.get("volume_24h") or 0),
            "Open Interest":    float(m.get("open_interest") or 0),
            "Funding Rate":     float(m.get("funding_rate") or 0) if m.get("funding_rate") else 0,
            "Change 24h (%)":   float(m.get("price_change_rate_24h") or 0) * 100,
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/paradex_cache.parquet", index=False)
    print(f"Paradex: {len(df)} пар збережено")

# ===========================================================================
# 5. Variational Omni — metadata/stats
# ===========================================================================

def fetch_variational():
    r = requests.get(
        "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats",
        timeout=20,
    )
    r.raise_for_status()
    rows = []
    for l in r.json().get("listings", []):
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
    df.to_parquet("data/variational_cache.parquet", index=False)
    print(f"Variational: {len(df)} пар збережено")

# ===========================================================================
# 6. Extended — info/markets з вкладеним marketStats
# ===========================================================================

def fetch_extended():
    r = requests.get("https://api.starknet.extended.exchange/api/v1/info/markets", timeout=20)
    r.raise_for_status()
    rows = []
    for m in r.json().get("data", []):
        if m.get("status") != "ACTIVE":
            continue
        s = m.get("marketStats", {})
        rows.append({
            "Pair":             m.get("name", ""),
            "Price (USD)":      float(s.get("lastPrice") or 0),
            "Volume 24h (USD)": float(s.get("dailyVolume") or 0),
            "Open Interest":    float(s.get("openInterest") or 0),
            "Funding Rate":     float(s.get("fundingRate") or 0),
            "Change 24h (%)":   float(s.get("dailyPriceChangePercentage") or 0) * 100,
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/extended_cache.parquet", index=False)
    print(f"Extended: {len(df)} пар збережено")

# ===========================================================================
# 7. Pacifica — info/prices
# ===========================================================================

def fetch_pacifica():
    r = requests.get("https://api.pacifica.fi/api/v1/info/prices", timeout=20)
    r.raise_for_status()
    rows = []
    for p in r.json().get("data", []):
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
    df.to_parquet("data/pacifica_cache.parquet", index=False)
    print(f"Pacifica: {len(df)} пар збережено")

# ===========================================================================
# 8. GRVT — N+1: спершу all_instruments, потім ticker по кожному (паралельно)
# ===========================================================================

def _grvt_one(instrument):
    try:
        r = requests.post(
            "https://market-data.grvt.io/full/v1/ticker",
            json={"instrument": instrument},
            timeout=10,
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
            "Volume 24h (USD)": buy_q + sell_q,
            "Open Interest":    float(d.get("open_interest") or 0),
            "Funding Rate":     float(d.get("funding_rate") or 0),
            "Change 24h (%)":   change_pct,
            "High 24h":         float(d.get("high_price") or 0),
            "Low 24h":          float(d.get("low_price") or 0),
        }
    except Exception:
        return None

def fetch_grvt():
    r = requests.post(
        "https://market-data.grvt.io/full/v1/all_instruments",
        json={"is_active": True},
        timeout=15,
    )
    r.raise_for_status()
    instruments = [i["instrument"] for i in r.json().get("result", [])]
    rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for res in ex.map(_grvt_one, instruments):
            if res:
                rows.append(res)
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/grvt_cache.parquet", index=False)
    print(f"GRVT: {len(df)} пар збережено")

# ===========================================================================
# 9. EdgeX — N+1: спершу getMetaData → contractList, потім getTicker по кожному
# ===========================================================================

def _edgex_one(c):
    try:
        cid = c.get("contractId")
        r = requests.get(
            "https://pro.edgex.exchange/api/v1/public/quote/getTicker",
            params={"contractId": cid},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json().get("data", [])
        if not data:
            return None
        t = data[0]
        return {
            "Pair":             c.get("contractName", ""),
            "Price (USD)":      float(t.get("lastPrice") or 0),
            "Volume 24h (USD)": float(t.get("value") or 0),
            "Open Interest":    float(t.get("openInterest") or 0),
            "Funding Rate":     float(t.get("fundingRate") or 0),
            "Change 24h (%)":   float(t.get("priceChangePercent") or 0) * 100,
            "Trades 24h":       int(float(t.get("trades") or 0)),
            "High 24h":         float(t.get("high") or 0),
            "Low 24h":          float(t.get("low") or 0),
        }
    except Exception:
        return None

def fetch_edgex():
    r = requests.get("https://pro.edgex.exchange/api/v1/public/meta/getMetaData", timeout=15)
    r.raise_for_status()
    contracts = r.json().get("data", {}).get("contractList", [])
    rows = []
    with ThreadPoolExecutor(max_workers=15) as ex:
        for res in ex.map(_edgex_one, contracts):
            if res:
                rows.append(res)
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/edgex_cache.parquet", index=False)
    print(f"EdgeX: {len(df)} пар збережено")

# ===========================================================================
# 10. ApeX Omni — N+1: спершу /symbols, потім /ticker по кожному
# ===========================================================================

def _apex_one(symbol_with_dash):
    """Symbol у /symbols має формат 'BTC-USDT', а /ticker очікує 'BTCUSDT'."""
    sym_no_dash = symbol_with_dash.replace("-", "")
    try:
        r = requests.get(
            "https://omni.apex.exchange/api/v3/ticker",
            params={"symbol": sym_no_dash},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json().get("data", [])
        if not data:
            return None
        t = data[0]
        return {
            "Pair":             symbol_with_dash,
            "Price (USDT)":     float(t.get("lastPrice") or 0),
            "Volume 24h (USD)": float(t.get("turnover24h") or 0),
            "Open Interest":    float(t.get("openInterest") or 0),
            "Funding Rate":     float(t.get("fundingRate") or 0),
            "Change 24h (%)":   float(t.get("price24hPcnt") or 0) * 100,
            "High 24h":         float(t.get("highPrice24h") or 0),
            "Low 24h":          float(t.get("lowPrice24h") or 0),
        }
    except Exception:
        return None

def fetch_apex():
    r = requests.get("https://omni.apex.exchange/api/v3/symbols", timeout=15)
    r.raise_for_status()
    perps = r.json().get("data", {}).get("contractConfig", {}).get("perpetualContract", []) or []
    symbols = [p["symbol"] for p in perps if p.get("symbol")]
    rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for res in ex.map(_apex_one, symbols):
            if res:
                rows.append(res)
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/apex_cache.parquet", index=False)
    print(f"ApeX Omni: {len(df)} пар збережено")

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
]

if __name__ == "__main__":
    print("Починаємо збір даних з 10 бірж...")
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
