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
# Категорії активів — кольори + емоджі + порядок (для легенд і teaser-bar)
# ---------------------------------------------------------------------------

CATEGORY_COLORS = {
    "Crypto":      "#3B82F6",   # 🟦
    "Stocks":      "#F97316",   # 🟧
    "Commodities": "#92400E",   # 🟫
    "FX":          "#EAB308",   # 🟨
    "Indices":     "#A855F7",   # 🟪
}

CATEGORY_EMOJI = {
    "Crypto":      "🟦",
    "Stocks":      "🟧",
    "Commodities": "🟫",
    "FX":          "🟨",
    "Indices":     "🟪",
}

# Порядок у teaser-bar: за обсягом у типовому снапшоті (Crypto найбільший, FX найменший)
CATEGORY_ORDER = ["Crypto", "Commodities", "Indices", "Stocks", "FX"]

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
# Глобальне сортування бірж за 24h volume (один раз, до рендеру)
# Цей порядок використовується скрізь: sidebar, картки, графіки, cross-table
# ---------------------------------------------------------------------------

ALL_DFS = {name: load_exchange(name) for name in EXCHANGES}
VOLUMES = {name: df["Volume 24h (USD)"].sum() if not df.empty else 0
           for name, df in ALL_DFS.items()}
# Біржі у порядку убування volume — найбільша зверху
SORTED_EXCHANGES = sorted(EXCHANGES.keys(), key=lambda n: VOLUMES[n], reverse=True)

# ---------------------------------------------------------------------------
# Sidebar — три режими навігації через явний page state:
#   "overview"   → головна сторінка з cross-exchange метриками
#   "categories" → 5 табів по категоріях з зведеними таблицями по активу
#   "exchange"   → детальна сторінка обраної біржі (nav_exchange != None)
# ---------------------------------------------------------------------------

st.sidebar.title("Perp DEX")

# Ініціалізація page state
if "page" not in st.session_state:
    st.session_state.page = "overview"

def _go_to_overview():
    st.session_state.page = "overview"
    st.session_state.nav_exchange = None

def _go_to_categories():
    st.session_state.page = "categories"
    st.session_state.nav_exchange = None

def _on_exchange_click():
    # Коли користувач обирає біржу у radio — переходимо у режим exchange
    if st.session_state.get("nav_exchange"):
        st.session_state.page = "exchange"

st.sidebar.button(
    "📊 Overview",
    on_click=_go_to_overview,
    use_container_width=True,
)
st.sidebar.button(
    "🎨 Categories",
    on_click=_go_to_categories,
    use_container_width=True,
)

st.sidebar.markdown("### 🏛 Exchanges")

selected_exchange = st.sidebar.radio(
    label="exchanges",
    options=SORTED_EXCHANGES,
    label_visibility="collapsed",
    index=None,
    key="nav_exchange",
    on_change=_on_exchange_click,
)

# ===========================================================================
# OVERVIEW (за замовчуванням)
# ===========================================================================

if st.session_state.page == "overview":
    st.title("Perp DEX — Market Data")
    st.caption("Cross-exchange analytics · cached locally")

    dfs = ALL_DFS  # вже завантажено вище

    errors = [name for name, df in dfs.items() if df.empty]
    if errors:
        st.warning(
            f"No data file for: {', '.join(errors)}. "
            f"Run fetch via GitHub Actions."
        )

    # Volumes у відсортованому порядку
    volumes = {name: VOLUMES[name] for name in SORTED_EXCHANGES}
    vol_total = sum(volumes.values())

    # ---- Combined DF: об'єднана таблиця всіх бірж для агрегатних метрик ----
    # Додаємо колонку 'Exchange' щоб розрізняти походження рядків.
    combined_df = pd.concat(
        [df.assign(Exchange=name) for name, df in dfs.items() if not df.empty],
        ignore_index=True,
    ) if any(not df.empty for df in dfs.values()) else pd.DataFrame()

    total_pairs = len(combined_df)
    unique_assets = combined_df["Asset Name"].dropna().nunique() if not combined_df.empty else 0

    # ---- Hero metrics: 3 колонки зверху ----
    st.subheader("Total Volume 24h")
    h1, h2, h3 = st.columns(3)
    h1.metric(f"All {len(EXCHANGES)} exchanges", fmt_usd(vol_total))
    h2.metric("Active PERP pairs", f"{total_pairs:,}")
    h3.metric("Unique assets", f"{unique_assets:,}")

    # Сітка 5×2 з біржами (10 бірж = 2 ряди по 5)
    items = list(volumes.items())
    for row_start in range(0, len(items), 5):
        row_items = items[row_start:row_start + 5]
        cols = st.columns(5)
        for i, (name, vol) in enumerate(row_items):
            cols[i].metric(name, fmt_usd(vol))

    st.divider()

    # ---- Asset Class Breakdown — horizontal bar (teaser для Categories page) ----
    st.subheader("Asset Class Breakdown")
    st.caption("Share of total 24h volume by category")

    if not combined_df.empty:
        cat_vol = (
            combined_df.groupby("Category")["Volume 24h (USD)"]
            .sum()
            .reindex(CATEGORY_ORDER)
            .dropna()
        )
        cat_total = cat_vol.sum()

        # Horizontal stacked bar — 1 рядок, 5 кольорових сегментів
        teaser_rows = []
        for cat, vol in cat_vol.items():
            pct = vol / cat_total * 100 if cat_total else 0
            teaser_rows.append({
                "Category": cat,
                "Volume":   vol,
                "Share":    pct,
                "Label":    f"{CATEGORY_EMOJI[cat]} {cat} · {pct:.2f}%",
            })
        teaser_df = pd.DataFrame(teaser_rows)

        fig_teaser = px.bar(
            teaser_df,
            x="Volume", y=["All categories"] * len(teaser_df),
            color="Category", color_discrete_map=CATEGORY_COLORS,
            orientation="h",
            hover_data={"Share": ":.2f", "Volume": ":,.0f"},
            text="Label",
            category_orders={"Category": CATEGORY_ORDER},
        )
        fig_teaser.update_traces(textposition="inside", insidetextanchor="middle")
        fig_teaser.update_layout(
            height=180, showlegend=True,
            xaxis_title=None, yaxis_title=None,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        fig_teaser.update_xaxes(showticklabels=False, showgrid=False)
        fig_teaser.update_yaxes(showticklabels=False)
        st.plotly_chart(fig_teaser, use_container_width=True)

        # ---- Global distribution table — ті самі дані, але у табличному вигляді ----
        dist_stats = (
            combined_df.groupby("Category")
            .agg(
                Pairs=("Pair", "count"),
                UniqueAssets=("Asset Name", "nunique"),
                Volume=("Volume 24h (USD)", "sum"),
            )
            .reindex(CATEGORY_ORDER)
            .dropna()
        )
        dist_total = dist_stats["Volume"].sum()
        dist_rows = []
        for cat, row in dist_stats.iterrows():
            dist_rows.append({
                "Category":      f"{CATEGORY_EMOJI[cat]} {cat}",
                "Pair Count":    int(row["Pairs"]),
                "Unique Assets": int(row["UniqueAssets"]),
                "Volume 24h":    row["Volume"],
                "% of Total":    row["Volume"] / dist_total * 100 if dist_total else 0,
            })
        # Додаємо підсумковий рядок
        dist_rows.append({
            "Category":      "**Total**",
            "Pair Count":    int(dist_stats["Pairs"].sum()),
            "Unique Assets": int(combined_df["Asset Name"].dropna().nunique()),
            "Volume 24h":    dist_total,
            "% of Total":    100.0,
        })
        dist_df = pd.DataFrame(dist_rows)
        st.dataframe(
            dist_df, use_container_width=True, hide_index=True,
            column_config={
                "Pair Count":    st.column_config.NumberColumn(format="%d"),
                "Unique Assets": st.column_config.NumberColumn(format="%d"),
                "Volume 24h":    USD_FMT,
                "% of Total":    PCT_FMT,
            },
        )

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
    st.caption("Pairs traded simultaneously on ≥3 exchanges")

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
        # Reorder: Pair, Exchanges, Total Volume, потім біржі у порядку убування volume
        col_order = ["Pair", "Exchanges", "Total Volume"] + SORTED_EXCHANGES
        cross_df = cross_df[[c for c in col_order if c in cross_df.columns]]
        st.dataframe(
            cross_df, use_container_width=True, hide_index=True,
            column_config=col_config,
        )
    else:
        st.info("No common pairs found across ≥3 exchanges.")

# ===========================================================================
# CATEGORIES — 5 вкладок (Crypto / Stocks / Commodities / FX / Indices)
# ===========================================================================

elif st.session_state.page == "categories":
    st.title("Asset Categories")
    st.caption("Cross-exchange breakdown by asset, grouped by category")

    # Об'єднуємо всі біржі в один DF з колонкою Exchange
    all_combined = pd.concat(
        [df.assign(Exchange=name) for name, df in ALL_DFS.items() if not df.empty],
        ignore_index=True,
    ) if any(not df.empty for df in ALL_DFS.values()) else pd.DataFrame()

    if all_combined.empty:
        st.error("No data available. Run fetch via GitHub Actions.")
    else:
        # Fallback: якщо у parquet немає колонки Display Asset (стара версія даних) —
        # будуємо її "на льоту" через той самий словник ASSET_GROUPS.
        # Це дозволяє сторінці працювати одразу після commit app.py, не чекаючи re-run fetch.yml.
        if "Display Asset" not in all_combined.columns:
            try:
                from categorize import ASSET_GROUPS, display_asset
            except ImportError:
                # Якщо categorize.py ще не деплоєно — безпечний мінімальний словник
                ASSET_GROUPS = {
                    "XAU": "Gold", "GOLD": "Gold", "XAUT": "Gold", "PAXG": "Gold",
                    "CL": "Crude Oil", "WTI": "Crude Oil", "BRENTOIL": "Crude Oil",
                    "XBR": "Crude Oil", "BZ": "Crude Oil", "OIL": "Crude Oil", "USOIL": "Crude Oil",
                    "XAG": "Silver", "SILVER": "Silver",
                    "XPT": "Platinum", "PLATINUM": "Platinum",
                    "XPD": "Palladium", "PALLADIUM": "Palladium",
                    "XCU": "Copper", "COPPER": "Copper",
                    "NATGAS": "Natural Gas", "GAS": "Natural Gas", "XNG": "Natural Gas",
                    "SP500": "S&P 500", "SPX": "S&P 500", "ES": "S&P 500", "SPX500": "S&P 500",
                    "QQQ": "Nasdaq-100", "NDX": "Nasdaq-100", "NASDAQ100": "Nasdaq-100",
                    "GOOGL": "Alphabet (Google)", "GOOG": "Alphabet (Google)",
                }
                def display_asset(canon, name):
                    if not canon:
                        return name
                    return ASSET_GROUPS.get(str(canon).upper(), name)

            all_combined["Display Asset"] = [
                display_asset(c, n)
                for c, n in zip(
                    all_combined["Canonical Base"].fillna(""),
                    all_combined["Asset Name"].fillna(""),
                )
            ]

        # ---- #4 TradFi-share per exchange (stacked horizontal bar) ----
        st.subheader("TradFi vs Crypto — share per exchange")
        st.caption("TradFi = Stocks + Commodities + Indices + FX combined")

        all_combined["AssetClass"] = all_combined["Category"].apply(
            lambda c: "Crypto" if c == "Crypto" else "TradFi"
        )
        share_df = (
            all_combined.groupby(["Exchange", "AssetClass"])["Volume 24h (USD)"]
            .sum()
            .reset_index()
        )
        # Впорядковуємо exchange'і за загальним volume (найбільша зверху)
        exch_totals = share_df.groupby("Exchange")["Volume 24h (USD)"].sum().sort_values(ascending=True)
        share_df["Exchange"] = pd.Categorical(
            share_df["Exchange"], categories=exch_totals.index.tolist(), ordered=True
        )

        fig_share = px.bar(
            share_df.sort_values("Exchange"),
            x="Volume 24h (USD)", y="Exchange",
            color="AssetClass",
            color_discrete_map={"Crypto": CATEGORY_COLORS["Crypto"], "TradFi": "#92400E"},
            orientation="h",
            barmode="stack",
            hover_data={"Volume 24h (USD)": ":,.0f"},
        )
        fig_share.update_layout(
            height=400, xaxis_title="24h Volume (USD)", yaxis_title=None,
            legend_title_text="", margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_share, use_container_width=True)

        # Короткий summary під bar — хто найбільш диверсифікований
        summary = (
            share_df.pivot(index="Exchange", columns="AssetClass", values="Volume 24h (USD)")
            .fillna(0)
            .assign(
                Total=lambda d: d.sum(axis=1),
                TradFi_pct=lambda d: d.get("TradFi", 0) / d.sum(axis=1) * 100,
            )
            .sort_values("TradFi_pct", ascending=False)
        )
        top_tradfi = summary.head(3).reset_index()
        top_tradfi_list = [
            f"**{row['Exchange']}** ({row['TradFi_pct']:.1f}%)"
            for _, row in top_tradfi.iterrows()
        ]
        st.caption(f"Most diversified into TradFi: {' · '.join(top_tradfi_list)}")

        st.divider()

        # ---- #5 Treemap: Category → Exchange → Volume ----
        st.subheader("Volume landscape — Category × Exchange")
        st.caption("Size of each block is proportional to 24h volume")

        tree_df = (
            all_combined.groupby(["Category", "Exchange"])["Volume 24h (USD)"]
            .sum()
            .reset_index()
        )
        tree_df = tree_df[tree_df["Volume 24h (USD)"] > 0]
        # Додаємо emoji-префікси у назви категорій для читабельності treemap labels
        tree_df["Category"] = tree_df["Category"].apply(
            lambda c: f"{CATEGORY_EMOJI.get(c, '')} {c}"
        )

        # Map кольорів з префіксом
        tree_color_map = {
            f"{CATEGORY_EMOJI[cat]} {cat}": color
            for cat, color in CATEGORY_COLORS.items()
        }

        fig_tree = px.treemap(
            tree_df,
            path=["Category", "Exchange"],
            values="Volume 24h (USD)",
            color="Category",
            color_discrete_map=tree_color_map,
            hover_data={"Volume 24h (USD)": ":,.0f"},
        )
        fig_tree.update_traces(
            textinfo="label+value+percent parent",
            texttemplate="<b>%{label}</b><br>$%{value:,.0f}<br>%{percentParent:.1%}",
        )
        fig_tree.update_layout(
            height=550, margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_tree, use_container_width=True)

        st.divider()

        # ---- Вкладки у порядку зменшення volume (Crypto → Commodities → Indices → Stocks → FX) ----
        tab_labels = [f"{CATEGORY_EMOJI[cat]} {cat}" for cat in CATEGORY_ORDER]
        tabs = st.tabs(tab_labels)

        for i, cat in enumerate(CATEGORY_ORDER):
            with tabs[i]:
                sub = all_combined[all_combined["Category"] == cat].copy()

                if sub.empty:
                    st.info(f"No {cat} pairs found in current data.")
                    continue

                # Відкидаємо рядки без Display Asset (захист від NaN)
                sub = sub[sub["Display Asset"].notna() & (sub["Display Asset"] != "")]

                # ---- Hero-метрики: 3 картки зверху вкладки ----
                total_pairs   = len(sub)
                unique_assets = sub["Display Asset"].nunique()
                total_vol     = sub["Volume 24h (USD)"].sum()

                h1, h2, h3 = st.columns(3)
                h1.metric("Total Pairs",   f"{total_pairs:,}")
                h2.metric("Unique Assets", f"{unique_assets:,}")
                h3.metric("Volume 24h",    fmt_usd(total_vol))

                # ---- Зведена таблиця: group by Display Asset ----
                agg = (
                    sub.groupby("Display Asset")
                    .agg(
                        Volume=("Volume 24h (USD)", "sum"),
                        Exchanges=("Exchange", "nunique"),
                        Pairs=("Pair", "count"),
                    )
                    .sort_values("Volume", ascending=False)
                    .reset_index()
                )
                # Нумерація з префіксом emoji категорії — виглядає як у макеті (🟦 1, 🟦 2, ...)
                emoji = CATEGORY_EMOJI[cat]
                agg.insert(0, "#", [f"{emoji} {i+1}" for i in range(len(agg))])

                # Перейменовуємо колонки для дружнього заголовка
                agg = agg.rename(columns={
                    "Display Asset": "Asset",
                    "Volume":        "Volume 24h (USD)",
                })

                st.dataframe(
                    agg[["#", "Asset", "Pairs", "Exchanges", "Volume 24h (USD)"]],
                    use_container_width=True, hide_index=True,
                    column_config={
                        "#":                 st.column_config.TextColumn(width="small"),
                        "Asset":             st.column_config.TextColumn(),
                        "Pairs":             st.column_config.NumberColumn(format="%d"),
                        "Exchanges":         st.column_config.NumberColumn(format="%d"),
                        "Volume 24h (USD)":  USD_FMT,
                    },
                )
                st.caption(
                    f"Rows are groupable by **Asset** (e.g., Gold combines XAU + XAUT + PAXG). "
                    f"Click any column header to re-sort."
                )

# ===========================================================================
# EXCHANGE DETAIL
# ===========================================================================

elif st.session_state.page == "exchange" and selected_exchange:
    cfg = EXCHANGES[selected_exchange]
    df = load_exchange(selected_exchange)

    # Заголовок з логотипом
    logo_col, title_col = st.columns([1, 12], vertical_alignment="center")
    with logo_col:
        st.image(cfg["logo_url"], width=64)
    with title_col:
        st.markdown(
            f"# {selected_exchange}\n\n"
            f"🔗 [{cfg['trade_label']}]({cfg['trade_url']})"
        )

    st.divider()

    if df.empty:
        st.error(
            f"Data file `{cfg['parquet']}` not found. "
            f"Run fetch via GitHub Actions."
        )
    else:
        # ---- Hero metrics: 4 колонки ----
        exch_total_vol = df["Volume 24h (USD)"].sum()
        exch_unique = df["Asset Name"].dropna().nunique()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Active pairs",     len(df))
        c2.metric("Unique assets",    f"{exch_unique:,}")
        c3.metric("Top pair",         df["Pair"].iloc[0])
        c4.metric("Total volume 24h", fmt_usd(exch_total_vol))

        # ---- Mini Asset Class breakdown (горизонтальний bar для однієї біржі) ----
        if df["Category"].notna().any():
            mini_cat = (
                df.groupby("Category")["Volume 24h (USD)"]
                .sum()
                .reindex(CATEGORY_ORDER)
                .dropna()
            )
            mini_total = mini_cat.sum()
            if mini_total > 0:
                mini_rows = []
                for cat, vol in mini_cat.items():
                    pct = vol / mini_total * 100
                    mini_rows.append({
                        "Category": cat,
                        "Volume":   vol,
                        "Label":    f"{CATEGORY_EMOJI[cat]} {cat} · {pct:.1f}%",
                    })
                mini_df = pd.DataFrame(mini_rows)

                fig_mini = px.bar(
                    mini_df,
                    x="Volume", y=[selected_exchange] * len(mini_df),
                    color="Category", color_discrete_map=CATEGORY_COLORS,
                    orientation="h", text="Label",
                    category_orders={"Category": CATEGORY_ORDER},
                    hover_data={"Volume": ":,.0f"},
                )
                fig_mini.update_traces(textposition="inside", insidetextanchor="middle")
                fig_mini.update_layout(
                    height=140, showlegend=False,
                    xaxis_title=None, yaxis_title=None,
                    margin=dict(l=10, r=10, t=10, b=10),
                )
                fig_mini.update_xaxes(showticklabels=False, showgrid=False)
                fig_mini.update_yaxes(showticklabels=False)
                st.plotly_chart(fig_mini, use_container_width=True)

        st.divider()

        # =======================================================================
        # HIP-3 breakdown — ТІЛЬКИ для Hyperliquid
        # Radio: Detailed (усі пари) vs Aggregated (згорнуті по dex)
        # =======================================================================

        show_aggregated = False
        if selected_exchange == "Hyperliquid" and df["HIP-3 Dex"].notna().any():
            st.subheader("HIP-3 breakdown")
            st.caption(
                "Native — базовий orderbook Hyperliquid; "
                "HIP-3 dexes — під-біржі на інфраструктурі Hyperliquid (xyz = Trade.XYZ тощо)"
            )

            view_mode = st.radio(
                label="View mode",
                options=["Detailed (all pairs)", "Aggregated (by dex)"],
                horizontal=True,
                label_visibility="collapsed",
                key="hl_view_mode",
            )
            show_aggregated = (view_mode == "Aggregated (by dex)")

            if show_aggregated:
                # Агрегатна таблиця: рядок на кожен dex (включно з "native")
                agg_df = df.copy()
                agg_df["Dex"] = agg_df["HIP-3 Dex"].fillna("native")
                agg_table = (
                    agg_df.groupby("Dex")
                    .agg(
                        Pairs=("Pair", "count"),
                        Volume=("Volume 24h (USD)", "sum"),
                    )
                    .sort_values("Volume", ascending=False)
                    .reset_index()
                )
                # Додаємо "головну" категорію для кожного dex (найпопулярніша за volume)
                focus = (
                    agg_df.groupby(["Dex", "Category"])["Volume 24h (USD)"].sum()
                    .reset_index()
                    .sort_values("Volume 24h (USD)", ascending=False)
                    .drop_duplicates("Dex")
                    [["Dex", "Category"]]
                    .rename(columns={"Category": "Primary Category"})
                )
                agg_table = agg_table.merge(focus, on="Dex", how="left")

                st.dataframe(
                    agg_table,
                    use_container_width=True, hide_index=True,
                    column_config={
                        "Pairs":  st.column_config.NumberColumn(format="%d"),
                        "Volume": USD_FMT,
                    },
                )

        # =======================================================================
        # Category filter + bar chart + таблиця пар
        # =======================================================================

        if not show_aggregated:
            st.subheader("Pairs")

            # ---- Category filter: мультивибір по категоріях, присутніх у цій біржі ----
            categories_present = sorted(df["Category"].dropna().unique().tolist())
            if categories_present:
                # Формуємо опції з емоджі для UX
                cat_labels = {
                    cat: f"{CATEGORY_EMOJI.get(cat, '⬜')} {cat}"
                    for cat in categories_present
                }
                selected_cats = st.multiselect(
                    label="Filter by category",
                    options=categories_present,
                    default=categories_present,
                    format_func=lambda c: cat_labels[c],
                    label_visibility="collapsed",
                )
                if selected_cats:
                    df_view = df[df["Category"].isin(selected_cats)].copy()
                else:
                    df_view = df.iloc[0:0].copy()  # нічого не обрано — порожньо
            else:
                df_view = df.copy()

            if df_view.empty:
                st.info("No pairs matching the selected categories.")
            else:
                # ---- Bar chart: top-10 пар з df_view ----
                bar_kwargs = dict(
                    x="Pair", y="Volume 24h (USD)",
                    color=cfg["color_col"], color_continuous_scale="RdYlGn",
                    color_continuous_midpoint=0,
                )
                fig = px.bar(df_view.head(10), **bar_kwargs)
                fig.update_layout(xaxis_tickangle=-45, height=420)
                st.plotly_chart(fig, use_container_width=True)

                # ---- Таблиця пар: з новими колонками (Category, Asset Name, HIP-3 Dex) ----
                # Формуємо порядок колонок: ідентифікація → ціна/volume → ризики/OI → extras
                preferred_order = [
                    "Pair", "Base", "Canonical Base", "Asset Name",
                    "Category", "HIP-3 Dex",
                    cfg["price_col"], "Volume 24h (USD)", "Open Interest",
                    "Change 24h (%)", "Funding Rate",
                    "Trades 24h", "High 24h", "Low 24h",
                    "Quote Currency",
                ]
                col_order = [c for c in preferred_order if c in df_view.columns]
                # Додаємо колонки, яких немає у preferred_order (резерв на майбутнє)
                col_order += [c for c in df_view.columns if c not in col_order]
                df_display = df_view[col_order]

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
                    df_display, use_container_width=True, hide_index=True,
                    column_config=col_config,
                )
