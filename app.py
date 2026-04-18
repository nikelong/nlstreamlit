import streamlit as st
import requests
import pandas as pd
import plotly.express as px

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
    stats = data.get("order_book_stats", [])
    rows = []
    for s in stats:
        rows.append({
            "Пара": s.get("symbol", "") + "/USD",
            "Ціна": float(s.get("last_trade_price") or 0),
            "Обсяг 24h (USD)": float(s.get("daily_quote_token_volume") or 0),
            "Зміна 24h (%)": float(s.get("daily_price_change") or 0),
            "Угод 24h": int(s.get("daily_trades_count") or 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("Обсяг 24h (USD)", ascending=False).reset_index(drop=True)

# --- UI ---

st.title("Perp DEX — Live Market Data")

tab_hl, tab_aster, tab_lighter = st.tabs(["Hyperliquid", "Aster DEX", "Lighter"])

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
