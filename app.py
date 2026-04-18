import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- Конфігурація сторінки ---
st.set_page_config(page_title="Aster DEX", layout="wide")

# --- Константи ---
BASE_URL = "https://fapi.asterdex.com"

@st.cache_data(ttl=30)  # кеш на 30 секунд
def fetch_ticker_24h():
    """Отримати 24h ticker по всіх активних ринках."""
    resp = requests.get(f"{BASE_URL}/fapi/v1/ticker/24hr", timeout=10)
    resp.raise_for_status()
    return resp.json()

@st.cache_data(ttl=60)
def fetch_exchange_info():
    """Отримати список активних торгових пар."""
    resp = requests.get(f"{BASE_URL}/fapi/v1/exchangeInfo", timeout=10)
    resp.raise_for_status()
    return resp.json()

# --- Заголовок ---
st.title("Aster DEX — Live Market Data")
st.caption("Дані оновлюються кожні 30 секунд. Джерело: fapi.asterdex.com")

# --- Завантаження даних ---
with st.spinner("Завантаження даних з Aster API..."):
    try:
        tickers = fetch_ticker_24h()
    except Exception as e:
        st.error(f"Помилка API: {e}")
        st.stop()

# --- Перетворення на DataFrame ---
df = pd.DataFrame(tickers)

# Перетворення числових колонок
numeric_cols = ["lastPrice", "priceChangePercent", "volume", "quoteVolume",
                "highPrice", "lowPrice"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Відбираємо та перейменовуємо колонки
cols_map = {
    "symbol": "Пара",
    "lastPrice": "Ціна (USDT)",
    "priceChangePercent": "Зміна 24h (%)",
    "volume": "Об'єм (базовий)",
    "quoteVolume": "Об'єм (USDT)",
    "highPrice": "Max 24h",
    "lowPrice": "Min 24h",
}
df_display = df[[c for c in cols_map if c in df.columns]].rename(columns=cols_map)
df_display = df_display.sort_values("Об'єм (USDT)", ascending=False).reset_index(drop=True)

# --- Метрики верхнього рівня ---
col1, col2, col3 = st.columns(3)
col1.metric("Активних пар", len(df_display))
col2.metric(
    "Топ за об'ємом",
    df_display["Пара"].iloc[0] if not df_display.empty else "—"
)
col3.metric(
    "Загальний об'єм 24h (USDT)",
    f"${df_display['Об\\'єм (USDT)'].sum():,.0f}" if "Об'єм (USDT)" in df_display.columns else "—"
)

st.divider()

# --- Графік топ-10 пар за об'ємом ---
st.subheader("Топ-10 пар за об'ємом (24h, USDT)")
top10 = df_display.head(10)
if not top10.empty and "Об'єм (USDT)" in top10.columns:
    fig = px.bar(
        top10,
        x="Пара",
        y="Об'єм (USDT)",
        color="Зміна 24h (%)",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        labels={"Об'єм (USDT)": "Об'єм, USDT"},
    )
    fig.update_layout(xaxis_tickangle=-45, height=420)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Повна таблиця ---
st.subheader("Всі ринки")
st.dataframe(
    df_display,
    use_container_width=True,
    hide_index=True,
)
