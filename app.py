import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Perp DEX Markets", layout="wide")

# ---------------------------------------------------------------------------
# Конфіг бірж — все централізовано тут, додавання нової = +1 рядок
# ---------------------------------------------------------------------------

EXCHANGES = {
    "Hyperliquid": {
        "parquet":      "data/hl_cache.parquet",
        "source_label": "api.hyperliquid.xyz",
        "source_url":   "https://api.hyperliquid.xyz",
        "trade_label":  "Open Hyperliquid",
        "trade_url":    "https://app.hyperliquid.xyz/trade/BTC",
        "color":        "#72D5C8",
        "price_col":    "Price (USDC)",
        "color_col":    "Funding Rate",
        "color_format": "%.8f",
    },
    "Aster DEX": {
        "parquet":      "data/aster_cache.parquet",
        "source_label": "fapi.asterdex.com",
        "source_url":   "https://fapi.asterdex.com",
        "trade_label":  "Open Aster DEX",
        "trade_url":    "https://www.asterdex.com/en/trade/pro/futures/BTCUSDT",
        "color":        "#E8C99A",
        "price_col":    "Price (USDT)",
        "color_col":    "Change 24h (%)",
        "color_format": None,
    },
    "Lighter": {
        "parquet":      "data/lighter_cache.parquet",
        "source_label": "mainnet.zklighter.elliot.ai",
        "source_url":   "https://mainnet.zklighter.elliot.ai",
        "trade_label":  "Open Lighter",
        "trade_url":    "https://app.lighter.xyz",
        "color":        "#13141C",
        "price_col":    "Price",
        "color_col":    "Change 24h (%)",
        "color_format": None,
    },
}

OVERVIEW_KEY = "📊 Overview"

# ---------------------------------------------------------------------------
# Завантаження даних — універсальний loader для будь-якої біржі
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900)
def load_exchange(name: str) -> pd.DataFrame:
    """Читає parquet для біржі. Повертає порожній DataFrame, якщо файл не знайдено."""
    try:
        return pd.read_parquet(EXCHANGES[name]["parquet"])
    except Exception:
        return pd.DataFrame()

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
# Sidebar — головний навігатор: Overview + список бірж в одному radio
# ---------------------------------------------------------------------------

st.sidebar.title("Perp DEX")

# Ініціалізація стану перед callback-ами
if "active_section" not in st.session_state:
    st.session_state.active_section = "overview"

# Callback-и для миттєвого переключення активної секції при кліку
def _on_overview_click():
    st.session_state.active_section = "overview"

def _on_exchange_click():
    st.session_state.active_section = "exchange"

# Секція 1: Overview
overview_label = "📊 Overview"
if st.session_state.active_section == "overview":
    overview_label = f"**▸ {overview_label}**"
st.sidebar.markdown(f"### {overview_label}")

st.sidebar.radio(
    label="overview-nav",
    options=[OVERVIEW_KEY],
    label_visibility="collapsed",
    key="nav_overview",
    on_change=_on_overview_click,
)

st.sidebar.divider()

# Секція 2: Exchanges
exchanges_label = "🏛 Exchanges"
if st.session_state.active_section == "exchange":
    exchanges_label = f"**▸ {exchanges_label}**"
st.sidebar.markdown(f"### {exchanges_label}")

exchange_selected = st.sidebar.radio(
    label="exchange-nav",
    options=list(EXCHANGES.keys()),
    label_visibility="collapsed",
    key="nav_exchange",
    on_change=_on_exchange_click,
)

st.sidebar.divider()
st.sidebar.markdown("### ℹ️ Про дашборд")
st.sidebar.markdown(
    "Cross-exchange аналітика perpetual DEX. "
    "Дані оновлюються через GitHub Actions."
)

# ---------------------------------------------------------------------------
# Main content — рендеримо обраний розділ
# ---------------------------------------------------------------------------

st.title("Perp DEX — Market Data")

# ============================================================================
# Розділ: OVERVIEW
# ============================================================================

if st.session_state.active_section == "overview":
    st.caption("Cross-exchange analytics · дані з локального кешу")

    # Завантажити всі біржі
    dfs = {name: load_exchange(name) for name in EXCHANGES}

    errors = [name for name, df in dfs.items() if df.empty]
    if errors:
        st.warning(
            f"Файл не знайдено для: {', '.join(errors)}. "
            f"Запустіть fetch через GitHub Actions."
        )

    # Total volumes per exchange
    volumes = {name: df["Volume 24h (USD)"].sum() if not df.empty else 0
               for name, df in dfs.items()}
    vol_total = sum(volumes.values())

    st.subheader("Total Volume 24h")
    cols = st.columns(len(EXCHANGES) + 1)
    cols[0].metric(f"All {len(EXCHANGES)} exchanges", fmt_usd(vol_total))
    for i, (name, vol) in enumerate(volumes.items(), start=1):
        cols[i].metric(name, fmt_usd(vol))

    st.divider()

    # Pie: market share
    st.subheader("Market Share by Volume 24h")
    pie_df = pd.DataFrame({
        "Exchange": list(volumes.keys()),
        "Volume":   list(volumes.values()),
    })
    color_map = {name: cfg["color"] for name, cfg in EXCHANGES.items()}
    fig_pie = px.pie(
        pie_df, names="Exchange", values="Volume", color="Exchange",
        color_discrete_map=color_map, hole=0.45,
    )
    fig_pie.update_traces(textinfo="percent+label")
    fig_pie.update_layout(height=380, showlegend=False)
    st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # Top 10 pairs per exchange (grouped bar)
    st.subheader("Top 10 Pairs by Volume — per Exchange")
    combined_rows = []
    for name, df in dfs.items():
        if not df.empty:
            for _, row in df.head(10).iterrows():
                combined_rows.append({
                    "Exchange":         name,
                    "Pair":             get_base_symbol(row["Pair"]),
                    "Volume 24h (USD)": row["Volume 24h (USD)"],
                })
    if combined_rows:
        fig_grouped = px.bar(
            pd.DataFrame(combined_rows),
            x="Pair", y="Volume 24h (USD)", color="Exchange", barmode="group",
            color_discrete_map=color_map,
        )
        fig_grouped.update_layout(xaxis_tickangle=-45, height=450)
        st.plotly_chart(fig_grouped, use_container_width=True)

    st.divider()

    # Cross-exchange common pairs
    st.subheader("Cross-Exchange Comparison — Common Pairs")
    st.caption("Pairs traded simultaneously on multiple exchanges")

    syms_per_exchange = {
        name: {get_base_symbol(r["Pair"]): r["Volume 24h (USD)"]
               for _, r in df.iterrows()} if not df.empty else {}
        for name, df in dfs.items()
    }
    all_symbols = set()
    for syms in syms_per_exchange.values():
        all_symbols.update(syms)

    cross_rows = []
    for sym in sorted(all_symbols):
        row = {"Pair": sym}
        present_count = 0
        total_vol = 0
        for name, syms in syms_per_exchange.items():
            v = syms.get(sym)
            row[f"{name} (USD)"] = v
            if v is not None:
                present_count += 1
                total_vol += v
        if present_count >= 2:
            row["Total Volume"] = total_vol
            cross_rows.append(row)

    if cross_rows:
        col_config = {f"{name} (USD)": USD_FMT for name in EXCHANGES}
        col_config["Total Volume"] = USD_FMT
        st.dataframe(
            pd.DataFrame(cross_rows).sort_values("Total Volume", ascending=False).reset_index(drop=True),
            use_container_width=True, hide_index=True, column_config=col_config,
        )
    else:
        st.info("No common pairs found.")

# ============================================================================
# Розділ: EXCHANGE DETAIL (вибрана біржа з sidebar)
# ============================================================================

else:
    cfg = EXCHANGES[exchange_selected]
    df = load_exchange(exchange_selected)

    st.markdown(
        f"### {exchange_selected}\n\n"
        f"Source: [{cfg['source_label']}]({cfg['source_url']}) · "
        f"🔗 [{cfg['trade_label']}]({cfg['trade_url']})"
    )

    if df.empty:
        st.error(
            f"Файл `{cfg['parquet']}` не знайдено. "
            f"Запустіть fetch через GitHub Actions."
        )
    else:
        # 3 metrics
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",      len(df))
        c2.metric("Top pair",          df["Pair"].iloc[0])
        c3.metric("Total volume 24h",  fmt_usd(df["Volume 24h (USD)"].sum()))

        # Bar chart top 10
        bar_kwargs = dict(
            x="Pair", y="Volume 24h (USD)",
            color=cfg["color_col"], color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig = px.bar(df.head(10), **bar_kwargs)
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        # Dataframe з усіма парами
        col_config = {
            cfg["price_col"]:    PRICE_FMT,
            "Volume 24h (USD)":  USD_FMT,
            "Open Interest":     USD_FMT,
            "Change 24h (%)":    PCT_FMT,
            "Trades 24h":        st.column_config.NumberColumn(format="%d"),
            "High 24h":          PRICE_FMT,
            "Low 24h":           PRICE_FMT,
        }
        if cfg["color_format"]:
            col_config[cfg["color_col"]] = st.column_config.NumberColumn(format=cfg["color_format"])

        st.dataframe(
            df, use_container_width=True, hide_index=True,
            column_config=col_config,
        )
