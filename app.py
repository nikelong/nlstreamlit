import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Perp DEX Markets", layout="wide")

ASTER_BASE = "https://fapi.asterdex.com"
HL_BASE = "https://api.hyperliquid.xyz/info"
LIGHTER_BASE = "https://mainnet.zklighter.elliot.ai"

# --- Функції отримання даних ---

@st.cache_data(ttl=30)
def fetch_aster_df():
    resp = requests.get(f"{ASTER_BASE}/fapi/v1/ticker/24hr", timeout=10)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    for col in ["lastPrice", "priceChangePercent", "quoteVolume", "highPrice", "lowPrice"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    cols_map = {
        "symbol": "Пара",
        "lastPrice": "Ціна (USDT)",
        "priceChangePercent": "Зміна 24h (%)",
        "quoteVolume": "Обсяг (USDT)",
        "highPrice": "Max 24h",
        "lowPrice": "Min 24h",
    }
    df = df[[c for c in cols_map if c in df.columns]].rename(columns=cols_map)
    return df.sort_values("Обсяг (USDT)", ascending=False).reset_index(drop=True)

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
            "Пара": asset["name"] + "/USDC",
            "Ціна (USDC)": float(ctx.get("markPx") or 0),
            "Обсяг 24h (USDC)": float(ctx.get("dayNtlVlm") or 0),
            "Open Interest": float(ctx.get("openInterest") or 0),
            "Funding Rate": float(ctx.get("funding") or 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Обсяг 24h (USDC)", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=30)
def fetch_lighter_df():
    resp = requests.get(f"{LIGHTER_BASE}/api/v1/exchangeStats", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for s in data.get("order_book_stats", []):
        rows.append({
            "Пара": s.get("symbol", "") + "/USD",
            "Ціна": float(s.get("last_trade_price") or 0),
            "Обсяг 24h (USD)": float(s.get("daily_quote_token_volume") or 0),
            "Зміна 24h (%)": float(s.get("daily_price_change") or 0),
            "Угод 24h": int(s.get("daily_trades_count") or 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Обсяг 24h (USD)", ascending=False).reset_index(drop=True)

# --- Допоміжна функція для зведеного дашборду ---

def get_base_symbol(pair: str) -> str:
    """Витягнути базовий символ з назви пари (BTC/USDC → BTC)."""
    return pair.split("/")[0].upper()

# --- UI ---

st.title("Perp DEX — Live Market Data")

tab_overview, tab_hl, tab_aster, tab_lighter = st.tabs([
    "📊 Зведений дашборд", "Hyperliquid", "Aster DEX", "Lighter"
])

# ---- TAB: Зведений дашборд ----
with tab_overview:
    st.caption("Крос-біржова аналітика · дані оновлюються кожні 30 сек")

    with st.spinner("Завантаження даних з усіх бірж..."):
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
        st.warning(f"Помилка API: {err}")

    # --- Підсумкові метрики по біржах ---
    vol_hl = df_hl["Обсяг 24h (USDC)"].sum() if not df_hl.empty else 0
    vol_aster = df_aster["Обсяг (USDT)"].sum() if not df_aster.empty else 0
    vol_lighter = df_lighter["Обсяг 24h (USD)"].sum() if not df_lighter.empty else 0
    vol_total = vol_hl + vol_aster + vol_lighter

    st.subheader("Загальний обсяг 24h")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всього (3 біржі)", f"${vol_total:,.0f}")
    c2.metric("Hyperliquid", f"${vol_hl:,.0f}")
    c3.metric("Aster DEX", f"${vol_aster:,.0f}")
    c4.metric("Lighter", f"${vol_lighter:,.0f}")

    st.divider()

    # --- Кругова діаграма: частки ринку ---
    st.subheader("Частки ринку за обсягом 24h")
    pie_data = {
        "Біржа": ["Hyperliquid", "Aster DEX", "Lighter"],
        "Обсяг": [vol_hl, vol_aster, vol_lighter],
    }
    fig_pie = px.pie(
        pd.DataFrame(pie_data),
        names="Біржа",
        values="Обсяг",
        color="Біржа",
        color_discrete_map={
            "Hyperliquid": "#00C4FF",
            "Aster DEX": "#FF6B35",
            "Lighter": "#7B61FF",
        },
        hole=0.45,
    )
    fig_pie.update_traces(textinfo="percent+label")
    fig_pie.update_layout(height=380, showlegend=False)
    st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # --- Топ-10 пар кожної біржі на одному груповому барчарті ---
    st.subheader("Топ-10 пар за обсягом — по кожній біржі")

    combined_rows = []
    if not df_hl.empty:
        for _, row in df_hl.head(10).iterrows():
            combined_rows.append({
                "Біржа": "Hyperliquid",
                "Пара": get_base_symbol(row["Пара"]),
                "Обсяг 24h (USD)": row["Обсяг 24h (USDC)"],
            })
    if not df_aster.empty:
        for _, row in df_aster.head(10).iterrows():
            combined_rows.append({
                "Біржа": "Aster DEX",
                "Пара": get_base_symbol(row["Пара"]),
                "Обсяг 24h (USD)": row["Обсяг (USDT)"],
            })
    if not df_lighter.empty:
        for _, row in df_lighter.head(10).iterrows():
            combined_rows.append({
                "Біржа": "Lighter",
                "Пара": get_base_symbol(row["Пара"]),
                "Обсяг 24h (USD)": row["Обсяг 24h (USD)"],
            })

    if combined_rows:
        df_combined = pd.DataFrame(combined_rows)
        fig_grouped = px.bar(
            df_combined,
            x="Пара",
            y="Обсяг 24h (USD)",
            color="Біржа",
            barmode="group",
            color_discrete_map={
                "Hyperliquid": "#00C4FF",
                "Aster DEX": "#FF6B35",
                "Lighter": "#7B61FF",
            },
        )
        fig_grouped.update_layout(xaxis_tickangle=-45, height=450)
        st.plotly_chart(fig_grouped, use_container_width=True)

    st.divider()

    # --- Крос-біржова таблиця: спільні пари ---
    st.subheader("Крос-біржове порівняння — спільні пари")
    st.caption("Пари, що торгуються одночасно на кількох біржах")

    if not df_hl.empty and not df_aster.empty and not df_lighter.empty:
        hl_syms = {get_base_symbol(r["Пара"]): r["Обсяг 24h (USDC)"]
                   for _, r in df_hl.iterrows()}
        aster_syms = {get_base_symbol(r["Пара"]): r["Обсяг (USDT)"]
                      for _, r in df_aster.iterrows()}
        lighter_syms = {get_base_symbol(r["Пара"]): r["Обсяг 24h (USD)"]
                        for _, r in df_lighter.iterrows()}

        all_syms = sorted(set(hl_syms) | set(aster_syms) | set(lighter_syms))
        cross_rows = []
        for sym in all_syms:
            hl_v = hl_syms.get(sym)
            as_v = aster_syms.get(sym)
            lt_v = lighter_syms.get(sym)
            # показуємо лише ті, що є мінімум на двох біржах
            present = sum([hl_v is not None, as_v is not None, lt_v is not None])
            if present >= 2:
                cross_rows.append({
                    "Пара": sym,
                    "Hyperliquid": f"${hl_v:,.0f}" if hl_v else "—",
                    "Aster DEX": f"${as_v:,.0f}" if as_v else "—",
                    "Lighter": f"${lt_v:,.0f}" if lt_v else "—",
                    "Бірж": present,
                })

        if cross_rows:
            df_cross = pd.DataFrame(cross_rows).sort_values("Бірж", ascending=False)
            st.dataframe(df_cross, use_container_width=True, hide_index=True)
        else:
            st.info("Спільних пар не знайдено.")

# ---- TAB: Hyperliquid ----
with tab_hl:
    st.caption("Джерело: api.hyperliquid.xyz · оновлення кожні 30 сек")
    with st.spinner("Завантаження Hyperliquid..."):
        try:
            df_hl = fetch_hyperliquid_df()
        except Exception as e:
            st.error(f"Помилка Hyperliquid API: {e}")
            df_hl = pd.DataFrame()

    if not df_hl.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Активних пар", len(df_hl))
        c2.metric("Топ за обсягом", df_hl["Пара"].iloc[0])
        c3.metric("Загальний обсяг 24h", f"${df_hl['Обсяг 24h (USDC)'].sum():,.0f}")

        st.subheader("Топ-10 пар за обсягом (24h)")
        fig = px.bar(
            df_hl.head(10), x="Пара", y="Обсяг 24h (USDC)",
            color="Funding Rate", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Всі ринки")
        st.dataframe(df_hl, use_container_width=True, hide_index=True)

# ---- TAB: Aster DEX ----
with tab_aster:
    st.caption("Джерело: fapi.asterdex.com · оновлення кожні 30 сек")
    with st.spinner("Завантаження Aster DEX..."):
        try:
            df_aster = fetch_aster_df()
        except Exception as e:
            st.error(f"Помилка Aster API: {e}")
            df_aster = pd.DataFrame()

    if not df_aster.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Активних пар", len(df_aster))
        c2.metric("Топ за обсягом", df_aster["Пара"].iloc[0])
        c3.metric("Загальний обсяг 24h", f"${df_aster['Обсяг (USDT)'].sum():,.0f}")

        st.subheader("Топ-10 пар за обсягом (24h)")
        fig = px.bar(
            df_aster.head(10), x="Пара", y="Обсяг (USDT)",
            color="Зміна 24h (%)", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Всі ринки")
        st.dataframe(df_aster, use_container_width=True, hide_index=True)

# ---- TAB: Lighter ----
with tab_lighter:
    st.caption("Джерело: mainnet.zklighter.elliot.ai · оновлення кожні 30 сек")
    with st.spinner("Завантаження Lighter..."):
        try:
            df_lighter = fetch_lighter_df()
        except Exception as e:
            st.error(f"Помилка Lighter API: {e}")
            df_lighter = pd.DataFrame()

    if not df_lighter.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Активних пар", len(df_lighter))
        c2.metric("Топ за обсягом", df_lighter["Пара"].iloc[0])
        c3.metric("Загальний обсяг 24h", f"${df_lighter['Обсяг 24h (USD)'].sum():,.0f}")

        st.subheader("Топ-10 пар за обсягом (24h)")
        fig = px.bar(
            df_lighter.head(10), x="Пара", y="Обсяг 24h (USD)",
            color="Зміна 24h (%)", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Всі ринки")
        st.dataframe(df_lighter, use_container_width=True, hide_index=True)
