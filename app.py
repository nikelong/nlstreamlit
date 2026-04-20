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
    options=SORTED_EXCHANGES,
    label_visibility="collapsed",
    index=None,
    key="nav_exchange",
)

# ===========================================================================
# OVERVIEW (за замовчуванням, якщо біржа не обрана)
# ===========================================================================

if selected_exchange is None:
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
