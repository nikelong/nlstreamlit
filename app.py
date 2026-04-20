import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Perp DEX Markets", layout="wide")

# ---------------------------------------------------------------------------
# Читання parquet-кешу
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900)
def load_hyperliquid():
    return pd.read_parquet("data/hl_cache.parquet")

@st.cache_data(ttl=900)
def load_aster():
    return pd.read_parquet("data/aster_cache.parquet")

@st.cache_data(ttl=900)
def load_lighter():
    return pd.read_parquet("data/lighter_cache.parquet")

# ---------------------------------------------------------------------------
# Column configs
# ---------------------------------------------------------------------------

USD_FMT   = st.column_config.NumberColumn(format="$%,.0f")
PCT_FMT   = st.column_config.NumberColumn(format="%.2f%%")
PRICE_FMT = st.column_config.NumberColumn(format="$%.4f")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BUSD", "PERP")

def get_base_symbol(pair: str) -> str:
    s = pair.split("/")[0].upper()
    for suffix in _QUOTE_SUFFIXES:
        if s.endswith(suffix) and len(s) > len(suffix):
            return s[: -len(suffix)]
    return s

def fmt_usd(val) -> str:
    try:
        return f"${int(round(float(val))):,}"
    except Exception:
        return "—"

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("Perp DEX — Market Data")

tab_overview, tab_hl, tab_aster, tab_lighter = st.tabs([
    "📊 Overview", "Hyperliquid", "Aster DEX", "Lighter"
])

# ---- TAB: Overview ----

with tab_overview:
    st.caption("Cross-exchange analytics · дані з локального кешу")

    errors = []
    try:
        df_hl = load_hyperliquid()
    except Exception as e:
        df_hl = pd.DataFrame()
        errors.append(f"Hyperliquid: {e}")
    try:
        df_aster = load_aster()
    except Exception as e:
        df_aster = pd.DataFrame()
        errors.append(f"Aster: {e}")
    try:
        df_lighter = load_lighter()
    except Exception as e:
        df_lighter = pd.DataFrame()
        errors.append(f"Lighter: {e}")

    if errors:
        for err in errors:
            st.warning(f"Файл не знайдено: {err}. Запустіть fetch через GitHub Actions.")

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
            "Hyperliquid": "#72D5C8",
            "Aster DEX":   "#E8C99A",
            "Lighter":     "#13141C",
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
                    "Exchange":         exch,
                    "Pair":             get_base_symbol(row["Pair"]),
                    "Volume 24h (USD)": row["Volume 24h (USD)"],
                })
    if combined_rows:
        fig_grouped = px.bar(
            pd.DataFrame(combined_rows),
            x="Pair", y="Volume 24h (USD)", color="Exchange", barmode="group",
            color_discrete_map={
                "Hyperliquid": "#72D5C8",
                "Aster DEX":   "#E8C99A",
                "Lighter":     "#13141C",
            },
        )
        fig_grouped.update_layout(xaxis_tickangle=-45, height=450)
        st.plotly_chart(fig_grouped, use_container_width=True)

    st.divider()
    st.subheader("Cross-Exchange Comparison — Common Pairs")
    st.caption("Pairs traded simultaneously on multiple exchanges")

    hl_syms      = {get_base_symbol(r["Pair"]): r["Volume 24h (USD)"] for _, r in df_hl.iterrows()}      if not df_hl.empty      else {}
    aster_syms   = {get_base_symbol(r["Pair"]): r["Volume 24h (USD)"] for _, r in df_aster.iterrows()}   if not df_aster.empty   else {}
    lighter_syms = {get_base_symbol(r["Pair"]): r["Volume 24h (USD)"] for _, r in df_lighter.iterrows()} if not df_lighter.empty else {}

    cross_rows = []
    for sym in sorted(set(hl_syms) | set(aster_syms) | set(lighter_syms)):
        hl_v = hl_syms.get(sym)
        as_v = aster_syms.get(sym)
        lt_v = lighter_syms.get(sym)
        if sum(v is not None for v in [hl_v, as_v, lt_v]) >= 2:
            cross_rows.append({
                "Pair":              sym,
                "Hyperliquid (USD)": hl_v,
                "Aster DEX (USD)":   as_v,
                "Lighter (USD)":     lt_v,
                "Total Volume":      (hl_v or 0) + (as_v or 0) + (lt_v or 0),
            })
    if cross_rows:
        st.dataframe(
            pd.DataFrame(cross_rows).sort_values("Total Volume", ascending=False).reset_index(drop=True),
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
        "🔗 [Open Hyperliquid](https://app.hyperliquid.xyz/trade/BTC)"
    )
    try:
        df_hl = load_hyperliquid()
    except Exception as e:
        st.error(f"Файл не знайдено: {e}. Запустіть fetch через GitHub Actions.")
        df_hl = pd.DataFrame()

    if not df_hl.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",      len(df_hl))
        c2.metric("Top pair",          df_hl["Pair"].iloc[0])
        c3.metric("Total volume 24h",  fmt_usd(df_hl["Volume 24h (USD)"].sum()))

        fig = px.bar(
            df_hl.head(10), x="Pair", y="Volume 24h (USD)",
            color="Funding Rate", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            df_hl, use_container_width=True, hide_index=True,
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
        "🔗 [Open Aster DEX](https://www.asterdex.com/en/trade/pro/futures/BTCUSDT)"
    )
    try:
        df_aster = load_aster()
    except Exception as e:
        st.error(f"Файл не знайдено: {e}. Запустіть fetch через GitHub Actions.")
        df_aster = pd.DataFrame()

    if not df_aster.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",     len(df_aster))
        c2.metric("Top pair",         df_aster["Pair"].iloc[0])
        c3.metric("Total volume 24h", fmt_usd(df_aster["Volume 24h (USD)"].sum()))

        fig = px.bar(
            df_aster.head(10), x="Pair", y="Volume 24h (USD)",
            color="Change 24h (%)", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            df_aster, use_container_width=True, hide_index=True,
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
        "🔗 [Open Lighter](https://app.lighter.xyz)"
    )
    try:
        df_lighter = load_lighter()
    except Exception as e:
        st.error(f"Файл не знайдено: {e}. Запустіть fetch через GitHub Actions.")
        df_lighter = pd.DataFrame()

    if not df_lighter.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",     len(df_lighter))
        c2.metric("Top pair",         df_lighter["Pair"].iloc[0])
        c3.metric("Total volume 24h", fmt_usd(df_lighter["Volume 24h (USD)"].sum()))

        fig = px.bar(
            df_lighter.head(10), x="Pair", y="Volume 24h (USD)",
            color="Change 24h (%)", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            df_lighter, use_container_width=True, hide_index=True,
            column_config={
                "Price":            PRICE_FMT,
                "Volume 24h (USD)": USD_FMT,
                "Change 24h (%)":   PCT_FMT,
                "Trades 24h":       st.column_config.NumberColumn(format="%d"),
            },
        )
