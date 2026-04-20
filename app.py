import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Perp DEX Markets", layout="wide")

# ---------------------------------------------------------------------------
# Конфіг 10 бірж — централізовано. Додавання нової = +1 словник.
# Поля:
#   parquet      — шлях до parquet-файлу
#   source_*     — посилання на API
#   trade_*      — посилання на сам додаток біржі
#   color        — колір біржі для графіків (узгоджений з її брендом)
#   price_col    — назва колонки з ціною (різна між біржами)
#   color_col    — колонка для розфарбовування bar chart
#   color_format — формат для color колонки в dataframe (None = за замовчуванням)
#   logo_url     — URL логотипу з CoinGecko CDN
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
        "logo_url":     "https://coin-images.coingecko.com/markets/images/1208/large/Hyperliquid_logo.png?1706865217",
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
        "logo_url":     "https://coin-images.coingecko.com/markets/images/22084/large/aster-profile-200.png?1756966162",
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
        "logo_url":     "https://coin-images.coingecko.com/markets/images/22097/large/lighter.jpg?1758003181",
    },
    "Paradex": {
        "parquet":      "data/paradex_cache.parquet",
        "source_label": "api.prod.paradex.trade",
        "source_url":   "https://api.prod.paradex.trade",
        "trade_label":  "Open Paradex",
        "trade_url":    "https://app.paradex.trade",
        "color":        "#9B59B6",
        "price_col":    "Price (USDC)",
        "color_col":    "Change 24h (%)",
        "color_format": None,
        "logo_url":     "https://coin-images.coingecko.com/markets/images/1310/large/paradex.jpg?1772694430",
    },
    "Variational Omni": {
        "parquet":      "data/variational_cache.parquet",
        "source_label": "omni-client-api.prod.ap-northeast-1.variational.io",
        "source_url":   "https://omni-client-api.prod.ap-northeast-1.variational.io",
        "trade_label":  "Open Variational",
        "trade_url":    "https://omni.variational.io",
        "color":        "#3498DB",
        "price_col":    "Price (USDC)",
        "color_col":    "Funding Rate",
        "color_format": "%.6f",
        "logo_url":     "https://coin-images.coingecko.com/markets/images/22231/large/Variational_Blue_Space-Blue-Mark-Space-Blue-Background.png?1773646635",
    },
    "Extended": {
        "parquet":      "data/extended_cache.parquet",
        "source_label": "api.starknet.extended.exchange",
        "source_url":   "https://api.starknet.extended.exchange",
        "trade_label":  "Open Extended",
        "trade_url":    "https://extended.exchange",
        "color":        "#E74C3C",
        "price_col":    "Price (USD)",
        "color_col":    "Change 24h (%)",
        "color_format": None,
        "logo_url":     "https://coin-images.coingecko.com/markets/images/22114/large/extended.png?1759117306",
    },
    "Pacifica": {
        "parquet":      "data/pacifica_cache.parquet",
        "source_label": "api.pacifica.fi",
        "source_url":   "https://api.pacifica.fi",
        "trade_label":  "Open Pacifica",
        "trade_url":    "https://app.pacifica.fi",
        "color":        "#1ABC9C",
        "price_col":    "Price (USD)",
        "color_col":    "Change 24h (%)",
        "color_format": None,
        "logo_url":     "https://coin-images.coingecko.com/markets/images/22171/large/Cyan_Logo_Dark_Background_%281%29.png?1764569549",
    },
    "GRVT": {
        "parquet":      "data/grvt_cache.parquet",
        "source_label": "market-data.grvt.io",
        "source_url":   "https://market-data.grvt.io",
        "trade_label":  "Open GRVT",
        "trade_url":    "https://grvt.io",
        "color":        "#F39C12",
        "price_col":    "Price (USDT)",
        "color_col":    "Change 24h (%)",
        "color_format": None,
        "logo_url":     "https://coin-images.coingecko.com/markets/images/11862/large/grvt.jpg?1768789669",
    },
    "EdgeX": {
        "parquet":      "data/edgex_cache.parquet",
        "source_label": "pro.edgex.exchange",
        "source_url":   "https://pro.edgex.exchange",
        "trade_label":  "Open EdgeX",
        "trade_url":    "https://pro.edgex.exchange",
        "color":        "#34495E",
        "price_col":    "Price (USD)",
        "color_col":    "Change 24h (%)",
        "color_format": None,
        "logo_url":     "https://coin-images.coingecko.com/markets/images/11726/large/Square.png?1775697036",
    },
    "ApeX Omni": {
        "parquet":      "data/apex_cache.parquet",
        "source_label": "omni.apex.exchange",
        "source_url":   "https://omni.apex.exchange",
        "trade_label":  "Open ApeX Omni",
        "trade_url":    "https://omni.apex.exchange",
        "color":        "#E67E22",
        "price_col":    "Price (USDT)",
        "color_col":    "Change 24h (%)",
        "color_format": None,
        "logo_url":     "https://coin-images.coingecko.com/markets/images/1669/large/100*100.PNG?1721201012",
    },
}

# ---------------------------------------------------------------------------
# Завантаження даних — універсальний loader для будь-якої біржі
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900)
def load_exchange(name: str) -> pd.DataFrame:
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
_DELIMITERS = "/-_"  # розділювачі: '/' Hyperliquid, '-' Paradex/ApeX, '_' GRVT

def get_base_symbol(pair: str) -> str:
    """Витягує базовий символ: 'BTC-USD-PERP' → 'BTC', 'BTC_USDT_Perp' → 'BTC', 'ETHUSDT' → 'ETH'."""
    # Спершу обрізаємо все після першого роздільника (/, -, _)
    s = pair
    for delim in _DELIMITERS:
        s = s.split(delim)[0]
    s = s.upper()
    # Потім обрізаємо суфікс quote-валюти, якщо він приклеєний без роздільника
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
# Sidebar — кнопка Overview (action) + radio для вибору біржі (selection)
# ---------------------------------------------------------------------------

st.sidebar.title("Perp DEX")

def _go_to_overview():
    st.session_state.nav_exchange = None

st.sidebar.button(
    "📊 Overview",
    on_click=_go_to_overview,
    use_container_width=True,
)

st.sidebar.markdown("### 🏛 Exchanges")

selected_exchange = st.sidebar.radio(
    label="exchanges",
    options=list(EXCHANGES.keys()),
    label_visibility="collapsed",
    index=None,
    key="nav_exchange",
)

st.sidebar.divider()
st.sidebar.markdown("### ℹ️ Про дашборд")
st.sidebar.markdown(
    "Cross-exchange аналітика 10 perpetual DEX. "
    "Дані оновлюються через GitHub Actions."
)

# ===========================================================================
# OVERVIEW (за замовчуванням, якщо біржа не обрана)
# ===========================================================================

if selected_exchange is None:
    st.title("Perp DEX — Market Data")
    st.caption("Cross-exchange analytics · дані з локального кешу")

    dfs = {name: load_exchange(name) for name in EXCHANGES}

    errors = [name for name, df in dfs.items() if df.empty]
    if errors:
        st.warning(
            f"Файл не знайдено для: {', '.join(errors)}. "
            f"Запустіть fetch через GitHub Actions."
        )

    volumes = {name: df["Volume 24h (USD)"].sum() if not df.empty else 0
               for name, df in dfs.items()}
    vol_total = sum(volumes.values())

    # Total volumes — total одним великим metric, інші — нижче в сітці
    st.subheader("Total Volume 24h")
    st.metric(f"All {len(EXCHANGES)} exchanges", fmt_usd(vol_total))

    # Сітка 5×2 з біржами (10 бірж = 2 ряди по 5)
    items = list(volumes.items())
    for row_start in range(0, len(items), 5):
        row_items = items[row_start:row_start + 5]
        cols = st.columns(5)
        for i, (name, vol) in enumerate(row_items):
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
    fig_pie.update_layout(height=450)
    st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # Top 10 pairs per exchange
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
        fig_grouped.update_layout(xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig_grouped, use_container_width=True)

    st.divider()

    # Cross-exchange common pairs (де торгується ≥3 біржах)
    st.subheader("Cross-Exchange Comparison — Common Pairs")
    st.caption("Пари, що торгуються одночасно на ≥3 біржах")

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
            row[f"{name}"] = v
            if v is not None:
                present_count += 1
                total_vol += v
        # ≥3 бірж — щоб не показувати нудний топ з BTC скрізь
        if present_count >= 3:
            row["Total Volume"] = total_vol
            row["Exchanges"]    = present_count
            cross_rows.append(row)

    if cross_rows:
        col_config = {f"{name}": USD_FMT for name in EXCHANGES}
        col_config["Total Volume"] = USD_FMT
        col_config["Exchanges"]    = st.column_config.NumberColumn(format="%d")
        cross_df = pd.DataFrame(cross_rows).sort_values("Total Volume", ascending=False).reset_index(drop=True)
        # Reorder: Pair, Exchanges, Total Volume, потім біржі
        col_order = ["Pair", "Exchanges", "Total Volume"] + list(EXCHANGES.keys())
        cross_df = cross_df[[c for c in col_order if c in cross_df.columns]]
        st.dataframe(
            cross_df, use_container_width=True, hide_index=True,
            column_config=col_config,
        )
    else:
        st.info("No common pairs found across ≥3 exchanges.")

# ===========================================================================
# EXCHANGE DETAIL
# ===========================================================================

else:
    cfg = EXCHANGES[selected_exchange]
    df = load_exchange(selected_exchange)

    # Заголовок з логотипом
    logo_col, title_col = st.columns([1, 12], vertical_alignment="center")
    with logo_col:
        st.image(cfg["logo_url"], width=64)
    with title_col:
        st.markdown(
            f"# {selected_exchange}\n\n"
            f"Source: [{cfg['source_label']}]({cfg['source_url']}) · "
            f"🔗 [{cfg['trade_label']}]({cfg['trade_url']})"
        )

    st.divider()

    if df.empty:
        st.error(
            f"Файл `{cfg['parquet']}` не знайдено. "
            f"Запустіть fetch через GitHub Actions."
        )
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Active pairs",      len(df))
        c2.metric("Top pair",          df["Pair"].iloc[0])
        c3.metric("Total volume 24h",  fmt_usd(df["Volume 24h (USD)"].sum()))

        bar_kwargs = dict(
            x="Pair", y="Volume 24h (USD)",
            color=cfg["color_col"], color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig = px.bar(df.head(10), **bar_kwargs)
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

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
