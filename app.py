import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Aster DEX", layout="wide")

BASE_URL = "https://fapi.asterdex.com"

@st.cache_data(ttl=30)
def fetch_ticker_24h():
    resp = requests.get(f"{BASE_URL}/fapi/v1/ticker/24hr", timeout=10)
    resp.raise_for_status()
    return resp.json()

st.title("Aster DEX — Live Market Data")
st.caption("Дані оновлюються кожні 30 секунд. Джерело: fapi.asterdex.com")

with st.spinner("Завантаження даних з Aster API..."):
    try:
        tickers = fetch_ticker_24h()
    except Exception as e:
        st.error(f"Помилка API: {e}")
        st.stop()

df = pd.DataFrame(tickers)

numeric_cols = ["lastPrice", "priceChangePercent", "volume", "quoteVolume",
                "highPrice", "lowPrice"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

cols_map = {
    "symbol": "Пара",
    "lastPrice": "Ціна (USDT)",
    "priceChangePercent": "Зміна 24h (%)",
    "volume": "Обсяг (базовий)",
    "quoteVolume": "Обсяг (USDT)",
    "highPrice": "Max 24h",
    "lowPrice": "Min 24h",
}
df_display = df[[c for c in cols_map if c in df.columns]].rename(columns=cols_map)
df_display = df_display.sort_values("Обсяг (USDT)", ascending=False).reset_index(drop=True)

col1, col2, col3 = st.columns(3)
col1.metric("Активних пар", len(df_display))
col2.metric("Топ за обсягом", df_display["Пара"].iloc[0] if not df_display.empty else "—")

total_volume = df_display["Обсяг (USDT)"].sum() if "Обсяг (USDT)" in df_display.columns else 0
col3.metric("Загальний обсяг 24h (USDT)", f"${total_volume:,.0f}")

st.divider()

st.subheader("Топ-10 пар за обсягом (24h, USDT)")
top10 = df_display.head(10)
if not top10.empty:
    fig = px.bar(
        top10,
        x="Пара",
        y="Обсяг (USDT)",
        color="Зміна 24h (%)",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        labels={"Обсяг (USDT)": "Обсяг, USDT"},
    )
    fig.update_layout(xaxis_tickangle=-45, height=420)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Всі ринки")
st.dataframe(df_display, use_container_width=True, hide_index=True)
