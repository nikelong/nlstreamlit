import os
import requests
import pandas as pd

os.makedirs("data", exist_ok=True)

# ---------------------------------------------------------------------------
# Hyperliquid
# ---------------------------------------------------------------------------

def fetch_hyperliquid():
    resp = requests.post(
        "https://api.hyperliquid.xyz/info",
        headers={"Content-Type": "application/json"},
        json={"type": "metaAndAssetCtxs"},
        timeout=15,
    )
    resp.raise_for_status()
    meta, ctxs = resp.json()
    rows = []
    for asset, ctx in zip(meta["universe"], ctxs):
        rows.append({
            "Pair":             asset["name"] + "/USDC",
            "Price (USDC)":     float(ctx.get("markPx") or 0),
            "Volume 24h (USD)": float(ctx.get("dayNtlVlm") or 0),
            "Open Interest":    float(ctx.get("openInterest") or 0),
            "Funding Rate":     float(ctx.get("funding") or 0),
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/hl_cache.parquet", index=False)
    print(f"Hyperliquid: {len(df)} пар збережено")

# ---------------------------------------------------------------------------
# Aster DEX
# ---------------------------------------------------------------------------

def fetch_aster():
    resp = requests.get(
        "https://fapi.asterdex.com/fapi/v1/ticker/24hr",
        timeout=15,
    )
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
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

# ---------------------------------------------------------------------------
# Lighter
# ---------------------------------------------------------------------------

def fetch_lighter():
    resp = requests.get(
        "https://mainnet.zklighter.elliot.ai/api/v1/exchangeStats",
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for s in data.get("order_book_stats", []):
        rows.append({
            "Pair":             s.get("symbol", "") + "/USD",
            "Price":            float(s.get("last_trade_price") or 0),
            "Volume 24h (USD)": float(s.get("daily_quote_token_volume") or 0),
            "Change 24h (%)":   float(s.get("daily_price_change") or 0),
            "Trades 24h":       int(s.get("daily_trades_count") or 0),
        })
    df = pd.DataFrame(rows).sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)
    df.to_parquet("data/lighter_cache.parquet", index=False)
    print(f"Lighter: {len(df)} пар збережено")

# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Починаємо збір даних...")
    fetch_hyperliquid()
    fetch_aster()
    fetch_lighter()
    print("Готово. Всі parquet-файли оновлено.")
