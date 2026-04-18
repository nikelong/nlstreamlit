import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Perp DEX Markets", layout="wide")

# --- Константи ---
ASTER_BASE = "https://fapi.asterdex.com"
HL_BASE = "https://api.hyperliquid.xyz/info"

# --- Функції отримання даних ---

@st.cache_data(ttl=30)
def fetch_aster():
    resp = requests.get(f"{ASTER_BASE}/fapi/v1/ticker/24hr", timeout=10)
    resp.raise_for_status()
    return resp.json()

@st.cache_data(ttl=30)
def fetch_hyperliquid():
    resp = requests.post(
        HL_BASE,
        headers={"Content-Type": "application/json"},
        json={"type": "metaAndAssetCtxs"},
        timeout=10,
    )
    resp.raise_for_status()
    meta, ctxs = resp.json()
    # meta["universe"] — список активів з назвами
    # ctxs — список контекстів у тому ж порядку
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
def fetch_aster_df():
    tickers = fetch_aster()
    df = pd.DataFrame(tickers)
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

# --- UI ---

st.title("Perp DEX — Live Market Data")

tab_hl, tab_aster = st.tabs(["Hyperliquid", "Aster DEX"])

# ---- TAB: Hyperliquid ----
with tab_hl:
    st.caption("Джерело: api.hyperliquid.xyz/info · оновлення кожні 30 сек")
    with st.spinner("Завантаження Hyperliquid..."):
        try:
            df_hl = fetch_hyperliquid()
        except Exception as e:
            st.error(f"Помилка Hyperliquid API: {e}")
            df_hl = pd.DataFrame()

    if not df_hl.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Активних пар", len(df_hl))
        c2.metric("Топ за обсягом", df_hl["Пара"].iloc[0])
        total_hl = df_hl["Обсяг 24h (USDC)"].sum()
        c3.metric("Загальний обсяг 24h", f"${total_hl:,.0f}")

        st.subheader("Топ-10 пар за обсягом (24h)")
        top10 = df_hl.head(10)
        fig = px.bar(
            top10,
            x="Пара",
            y="Обсяг 24h (USDC)",
            color="Funding Rate",
            color_continuous_scale="RdYlGn",
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
        total_aster = df_aster["Обсяг (USDT)"].sum()
        c3.metric("Загальний обсяг 24h", f"${total_aster:,.0f}")

        st.subheader("Топ-10 пар за обсягом (24h)")
        top10 = df_aster.head(10)
        fig = px.bar(
            top10,
            x="Пара",
            y="Обсяг (USDT)",
            color="Зміна 24h (%)",
            color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
        )
        fig.update_layout(xaxis_tickangle=-45, height=420)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Всі ринки")
        st.dataframe(df_aster, use_container_width=True, hide_index=True)
