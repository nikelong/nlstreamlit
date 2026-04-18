import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Perp DEX Markets", layout="wide")

ASTER_BASE = "https://fapi.asterdex.com"
HL_BASE = "https://api.hyperliquid.xyz/info"
LIGHTER_BASE = "https://mainnet.zklighter.elliot.ai"

# --- Data fetching ---

@st.cache_data(ttl=30)
def fetch_aster_df():
    resp = requests.get(f"{ASTER_BASE}/fapi/v1/ticker/24hr", timeout=10)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    for col in ["lastPrice", "priceChangePercent", "quoteVolume", "highPrice", "lowPrice"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    cols_map = {
        "symbol": "Pair",
        "lastPrice": "Price (USDT)",
        "priceChangePercent": "Change 24h (%)",
        "quoteVolume": "Volume 24h (USD)",
        "highPrice": "High 24h",
        "lowPrice": "Low 24h",
    }
    df = df[[c for c in cols_map if c in df.columns]].rename(columns=cols_map)
    return df.sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=30)
def fetch_hyperliquid_df():
    resp = requests.post(
        HL_BASE,
        headers={"Content-Type": "application/json"},
        json={"type": "metaAndAssetCtxs"},
        timeout=10,
    )
    resp.raise_for_status()
    meta, ctxs = resp.json()
    rows = []
    for asset, ctx in zip(meta["universe"], ctxs):
        rows.append({
            "Pair": asset["name"] + "/USDC",
            "Price (USDC)": float(ctx.get("markPx") or 0),
            "Volume 24h (USD)": float(ctx.get("dayNtlVlm") or 0),
            "Open Interest": float(ctx.get("openInterest") or 0),
            "Funding Rate": float(ctx.get("funding") or 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=30)
def fetch_lighter_df():
    resp = requests.get(f"{LIGHTER_BASE}/api/v1/exchangeStats", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for s in data.get("order_book_stats", []):
        rows.append({
            "Pair": s.get("symbol", "") + "/USD",
            "Price": float(s.get("last_trade_price") or 0),
            "Volume 24h (USD)": float(s.get("daily_quote_token_volume") or 0),
            "Change 24h (%)": float(s.get("daily_price_change") or 0),
            "Trades 24h": int(s.get("daily_trades_count") or 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)

# --- Helpers ---

def get_base_symbol(pair: str) -> str:
    return pair.split("/")[0].upper()

def fmt_usd(val) -> str:
    """Format a number as $1,234,567 (no decimals)."""
    try:
        return f"${int(round(float(val))):,}"
    except Exception:
        return "—"

def fmt_price(val) -> str:
    """Format price: no decimals for large values, up to 6 decimals for small."""
    try:
        v = float(val)
        if v == 0:
            return "—"
        if v >= 1:
            return f"${v:,.2f}"
        return f"${v:.6f}"
    except Exception:
        return "—"

def fmt_pct(val) -> str:
    try:
        return f"{float(val):+.2f}%"
    except Exception:
        return "—"

def apply_fmt(df: pd.DataFrame, price_cols: list, vol_cols: list, pct_cols: list) -> pd.DataFrame:
    df = df.copy()
    for c in price_cols:
        if c in df.columns:
            df[c] = df[c].apply(fmt_price)
    for c in vol_cols:
        if c in df.columns:
            df[c] = df[c].apply(fmt_usd)
    for c in pct_cols:
        if c in df.columns:
            df[c] = df[c].apply(fmt_pct)
    return df

# --- UI ---

st.title("Perp DEX — Live Market Data")

tab_overview, tab_hl, tab_aster, tab_lighter = st.tabs([
    "📊 Overview", "Hyperliquid", "Aster DEX", "Lighter"
])

# ---- TAB: Overview ----
with tab_overview:
    st.caption("Cross-exchange analytics · data refreshes every 30 sec")

    with st.spinner("Loading data from all exchanges..."):
        errors = []
        try:
            df_hl = fetch_hyperliquid_df()
        except Exception as e:
            df_hl = pd.DataFrame()
            errors.append(f"Hyperliquid: {e}")
        try:
            df_aster = fetch_aster_df()
        except Exception as e:
            df_aster = pd.DataFrame()
            errors.append(f"Aster: {e}")
        try:
            df_lighter = fetch_lighter_df()
        except Exception as e:
            df_lighter = pd.DataFrame()
            errors.append(f"Lighter: {e}")

    for err in errors:
        st.warning(f"API error: {err}")

    vol_hl     = df_hl["Volume 24h (USD)"].sum()     if not df_hl.empty     else 0
    vol_aster  = df_aster["Volume 24h (USD)"].sum()  if not df_aster.empty  else 0
    vol_lighter= df_lighter["Volume 24h (USD)"].sum() if not df_lighter.empty else 0
    vol_total  = vol_hl + vol_aster + vol_lighter

    st.subheader("Total Volume 24h")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("All 3 exchanges", fmt_usd(vol_total))
    c2.metric("Hyperliquid",     fmt_usd(vol_hl))
    c3.metric("Aster DEX",       fmt_usd(vol_aster))
    c4.metric("Lighter",         fmt_usd(vol_lighter))

    st.divider()

    st.subheader("Market Share by Volume 24h")
    pie_data = {
        "Exchange": ["Hyperliquid", "Aster DEX", "Lighter"],
        "Volume":   [vol_hl, vol_aster, vol_lighter],
    }
    fig_pie = px.pie(
        pd.DataFrame(pie_data),
        names="Exchange",
        values="Volume",
        color="Exchange",
        color_discrete_map={
            "Hyperliquid": "#00C4FF",
            "Aster DEX":   "#FF6B35",
            "Lighter":     "#7B61FF",
        },
        hole=0.45,
    )
    fig_pie.update_traces(textinfo="percent+label")
    fig_pie.update_layout(height=380, showlegend=False)
    st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    st.subheader("Top 10 Pairs by Volume — per Exchange")
    combined_rows = []
    if not df_hl.empty:
        for _, row in df_hl.head(10).iterrows():
            combined_rows.append({
                "Exchange": "Hyperliquid",
                "Pair": get_base_symbol(row["Pair"]),
                "Volume 24h (USD)": row["Volume 24h (USD)"],
            })
    if not df_aster.empty:
        for _, row in df_aster.head(10).iterrows():
            combined_rows.append({
                "Exchange": "Aster DEX",
                "Pair": get_base_symbol(row["Pair"]),
                "Volume 24h (USD)": row["Volume 24h (USD)"],
            })
    if not df_lighter.empty:
        for _, row in df_lighter.head(10).iterrows():
            combined_rows.append({
                "Exchange": "Lighter",
                "Pair": get_base_symbol(row["Pair"]),
                "Volume 24h (USD)": row["Volume 24h (USD)"],
            })

    if combined_rows:
        df_combined = pd.DataFrame(combined_rows)
        fig_grouped = px.bar(
            df_combined,
            x="Pair",
            y="Volume 24h (USD)",
            color="Exchange",
            barmode="group",
            color_discrete_map={
                "Hyperliquid": "#00C4FF",
                "Aster DEX":   "#FF6B35",
                "Lighter":     "#7B61FF",
            },
        )
        fig_grouped.update_layout(xaxis_tickangle=-45, height=450)
        st.plotly_chart(fig_grouped, use_container_width=True)

    st.divider()

    st.subheader("Cross-Exchange Comparison — Common Pairs")
    st.caption("Pairs traded simultaneously on multiple exchanges")

    if not df_hl.empty and not df_aster.empty and not df_lighter.empty:
        hl_syms      = {get_base_symbol(r["Pair"]): r["Volume 24h (USD)"] for _, r in df_hl.iterrows()}
        aster_syms   = {get_base_symbol(r["Pair"]): r["Volume 24h (USD)"] for _, r in df_aster.iterrows()}
        lighter_syms = {get_base_symbol(r["Pair"]): r["Volume 24h (USD)"] for _, r in df_lighter.iterrows()}

        all_syms = sorted(set(hl_syms) | set(aster_syms) | set(lighter_syms))
        cross_rows = []
        for sym in all_syms:
            hl_v  = hl_syms.get(sym)
            as_v  = aster_syms.get(sym)
            lt_v  = lighter_syms.get(sym)
            present = sum([hl_v is not None, as_v is not None, lt_v is not None])
            if present >= 2:
                total_v = (hl_v or 0) + (as_v or 0) + (lt_v or 0)
                cross_rows.append({
                    "Pair":        sym,
                    "Hyperliquid": fmt_usd(hl_v) if hl_v is not None else "—",
                    "Aster DEX":   fmt_usd(as_v) if as_v is not None else "—",
                    "Lighter":     fmt_usd(lt_v) if lt_v is not None else "—",
                    "Total Volume": fmt_usd(total_v),
                    "Exchanges":   present,
                })

        if cross_rows:
            # сортуємо по числовому total перед форматуванням — вже відформатовано, тож сортуємо по present desc
            df_cross = pd.DataFrame(cross_rows).sort_values("Exchanges", ascending=False)
            st.dataframe(df_cross, use_container_width=True, hide_index=True)
        else:
            st.info("No common pairs found.")

# ---- TAB: Hyperliquid ----
with tab_hl:
    st.markdown(
        "Source: [api.hyperliquid.xyz](https://api.hyperliquid.xyz) · "
        "🔗 [Open Hyperliquid](https://app.hyperliquid.xyz/trade/BTC) · "
        "data refreshes every 30 sec"
    )
    with st.spinner("Loading Hyperliquid..."):
        try:
            df_hl = fetch_hyperliquid_df()
        except Exception as e:
            st.error(f"Hyperliquid API error: {e}")
            df_hl = pd.DataFrame()

    if not df_hl.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",     len(df_hl))
        c2.metric("Top pair",         df_hl["Pair"].iloc[0])
        c3.metric("Total volume 24h", fmt_usd(df_hl["Volume 24h (USD)"].sum()))

        st.subheader("Top 10 Pairs by Volume (24h)")
        fig = px.bar(
            df_hl.head(10), x="Pair", y="Volume 24h (USD)",
            color="Funding Rate", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("All Markets")
        df_hl_fmt = apply_fmt(
            df_hl,
            price_cols=["Price (USDC)"],
            vol_cols=["Volume 24h (USD)", "Open Interest"],
            pct_cols=[],
        )
        # Funding Rate — окремо (маленькі числа, 8 знаків)
        df_hl_fmt["Funding Rate"] = df_hl["Funding Rate"].apply(
            lambda v: f"{float(v):.8f}" if v != 0 else "0.00000000"
        )
        st.dataframe(df_hl_fmt, use_container_width=True, hide_index=True)

# ---- TAB: Aster DEX ----
with tab_aster:
    st.markdown(
        "Source: [fapi.asterdex.com](https://fapi.asterdex.com) · "
        "🔗 [Open Aster DEX](https://www.asterdex.com/en/trade/pro/futures/BTCUSDT) · "
        "data refreshes every 30 sec"
    )
    with st.spinner("Loading Aster DEX..."):
        try:
            df_aster = fetch_aster_df()
        except Exception as e:
            st.error(f"Aster API error: {e}")
            df_aster = pd.DataFrame()

    if not df_aster.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",     len(df_aster))
        c2.metric("Top pair",         df_aster["Pair"].iloc[0])
        c3.metric("Total volume 24h", fmt_usd(df_aster["Volume 24h (USD)"].sum()))

        st.subheader("Top 10 Pairs by Volume (24h)")
        fig = px.bar(
            df_aster.head(10), x="Pair", y="Volume 24h (USD)",
            color="Change 24h (%)", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("All Markets")
        df_aster_fmt = apply_fmt(
            df_aster,
            price_cols=["Price (USDT)"],
            vol_cols=["Volume 24h (USD)", "High 24h", "Low 24h"],
            pct_cols=["Change 24h (%)"],
        )
        st.dataframe(df_aster_fmt, use_container_width=True, hide_index=True)

# ---- TAB: Lighter ----
with tab_lighter:
    st.markdown(
        "Source: [mainnet.zklighter.elliot.ai](https://mainnet.zklighter.elliot.ai) · "
        "🔗 [Open Lighter](https://app.lighter.xyz) · "
        "data refreshes every 30 sec"
    )
    with st.spinner("Loading Lighter..."):
        try:
            df_lighter = fetch_lighter_df()
        except Exception as e:
            st.error(f"Lighter API error: {e}")
            df_lighter = pd.DataFrame()

    if not df_lighter.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",     len(df_lighter))
        c2.metric("Top pair",         df_lighter["Pair"].iloc[0])
        c3.metric("Total volume 24h", fmt_usd(df_lighter["Volume 24h (USD)"].sum()))

        st.subheader("Top 10 Pairs by Volume (24h)")
        fig = px.bar(
            df_lighter.head(10), x="Pair", y="Volume 24h (USD)",
            color="Change 24h (%)", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("All Markets")
        df_lighter_fmt = apply_fmt(
            df_lighter,
            price_cols=["Price"],
            vol_cols=["Volume 24h (USD)"],
            pct_cols=["Change 24h (%)"],
        )
        st.dataframe(df_lighter_fmt, use_container_width=True, hide_index=True)
