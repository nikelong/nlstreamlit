import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Perp DEX Markets", layout="wide")

ASTER_BASE  = "https://fapi.asterdex.com"
HL_BASE     = "https://api.hyperliquid.xyz/info"
LIGHTER_BASE = "https://mainnet.zklighter.elliot.ai"

# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def fetch_aster_df():
    resp = requests.get(f"{ASTER_BASE}/fapi/v1/ticker/24hr", timeout=10)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    for col in ["lastPrice", "priceChangePercent", "quoteVolume", "highPrice", "lowPrice"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    cols_map = {
        "symbol":             "Pair",
        "lastPrice":          "Price (USDT)",
        "priceChangePercent": "Change 24h (%)",
        "quoteVolume":        "Volume 24h (USD)",
        "highPrice":          "High 24h",
        "lowPrice":           "Low 24h",
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
            "Pair":             asset["name"] + "/USDC",
            "Price (USDC)":    float(ctx.get("markPx") or 0),
            "Volume 24h (USD)": float(ctx.get("dayNtlVlm") or 0),
            "Open Interest":   float(ctx.get("openInterest") or 0),
            "Funding Rate":    float(ctx.get("funding") or 0),
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
            "Pair":             s.get("symbol", "") + "/USD",
            "Price":            float(s.get("last_trade_price") or 0),
            "Volume 24h (USD)": float(s.get("daily_quote_token_volume") or 0),
            "Change 24h (%)":   float(s.get("daily_price_change") or 0),
            "Trades 24h":       int(s.get("daily_trades_count") or 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Volume 24h (USD)", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Column configs — числа залишаються float, Streamlit форматує як $1,234,567
# ---------------------------------------------------------------------------

USD_FMT   = st.column_config.NumberColumn(format="$%d")         # обсяги — цілі
PCT_FMT   = st.column_config.NumberColumn(format="%.2f%%")      # відсотки
PRICE_FMT = st.column_config.NumberColumn(format="$%.4f")       # ціни — 4 знаки

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_base_symbol(pair: str) -> str:
    return pair.split("/")[0].upper()


def fmt_usd(val) -> str:
    """Для st.metric — де потрібен рядок."""
    try:
        return f"${int(round(float(val))):,}"
    except Exception:
        return "—"


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

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

    vol_hl      = df_hl["Volume 24h (USD)"].sum()      if not df_hl.empty      else 0
    vol_aster   = df_aster["Volume 24h (USD)"].sum()   if not df_aster.empty   else 0
    vol_lighter = df_lighter["Volume 24h (USD)"].sum() if not df_lighter.empty else 0
    vol_total   = vol_hl + vol_aster + vol_lighter

    st.subheader("Total Volume 24h")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("All 3 exchanges", fmt_usd(vol_total))
    c2.metric("Hyperliquid",     fmt_usd(vol_hl))
    c3.metric("Aster DEX",       fmt_usd(vol_aster))
    c4.metric("Lighter",         fmt_usd(vol_lighter))

    st.divider()

    st.subheader("Market Share by Volume 24h")
    fig_pie = px.pie(
        pd.DataFrame({
            "Exchange": ["Hyperliquid", "Aster DEX", "Lighter"],
            "Volume":   [vol_hl, vol_aster, vol_lighter],
        }),
        names="Exchange", values="Volume", color="Exchange",
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
    for source_df, exch in [(df_hl, "Hyperliquid"), (df_aster, "Aster DEX"), (df_lighter, "Lighter")]:
        if not source_df.empty:
            for _, row in source_df.head(10).iterrows():
                combined_rows.append({
                    "Exchange":          exch,
                    "Pair":              get_base_symbol(row["Pair"]),
                    "Volume 24h (USD)":  row["Volume 24h (USD)"],
                })
    if combined_rows:
        fig_grouped = px.bar(
            pd.DataFrame(combined_rows),
            x="Pair", y="Volume 24h (USD)", color="Exchange", barmode="group",
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

        cross_rows = []
        for sym in sorted(set(hl_syms) | set(aster_syms) | set(lighter_syms)):
            hl_v  = hl_syms.get(sym)
            as_v  = aster_syms.get(sym)
            lt_v  = lighter_syms.get(sym)
            present = sum(v is not None for v in [hl_v, as_v, lt_v])
            if present >= 2:
                cross_rows.append({
                    "Pair":              sym,
                    "Hyperliquid (USD)": hl_v  if hl_v  is not None else None,
                    "Aster DEX (USD)":   as_v  if as_v  is not None else None,
                    "Lighter (USD)":     lt_v  if lt_v  is not None else None,
                    "Total Volume":      (hl_v or 0) + (as_v or 0) + (lt_v or 0),
                    "Exchanges":         present,
                })

        if cross_rows:
            df_cross = (
                pd.DataFrame(cross_rows)
                .sort_values("Total Volume", ascending=False)  # числове сортування
                .reset_index(drop=True)
            )
            st.dataframe(
                df_cross,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Hyperliquid (USD)": USD_FMT,
                    "Aster DEX (USD)":   USD_FMT,
                    "Lighter (USD)":     USD_FMT,
                    "Total Volume":      USD_FMT,
                },
            )
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
        st.dataframe(
            df_hl,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Price (USDC)":     PRICE_FMT,
                "Volume 24h (USD)": USD_FMT,
                "Open Interest":    USD_FMT,
                "Funding Rate":     st.column_config.NumberColumn(format="%.8f"),
            },
        )

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
        st.dataframe(
            df_aster,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Price (USDT)":     PRICE_FMT,
                "Volume 24h (USD)": USD_FMT,
                "High 24h":         PRICE_FMT,
                "Low 24h":          PRICE_FMT,
                "Change 24h (%)":   PCT_FMT,
            },
        )

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
        st.dataframe(
            df_lighter,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Price":            PRICE_FMT,
                "Volume 24h (USD)": USD_FMT,
                "Change 24h (%)":   PCT_FMT,
                "Trades 24h":       st.column_config.NumberColumn(format="%d"),
            },
        )
