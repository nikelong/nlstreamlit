import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Aster", layout="wide")
st.title("📊 Aster DEX")
st.caption("Топ-пари за обсягом (24h) · Дані: Aster REST API v3")

@st.cache_data(ttl=60)
def fetch_tickers() -> pd.DataFrame:
    # V3 endpoint — NONE security, ключ не потрібен
    r = requests.get(
        "https://fapi3.asterdex.com/fapi/v3/ticker/24hr",
        timeout=10,
    )
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    for col in ["volume", "quoteVolume", "lastPrice", "priceChangePercent"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

try:
    df = fetch_tickers()
except Exception as e:
    st.error(f"Помилка запиту до Aster API: {e}")
    st.stop()

top20 = (
    df.sort_values("quoteVolume", ascending=False)
    .head(20)
    .reset_index(drop=True)
)

col1, col2, col3 = st.columns(3)
col1.metric("Інструментів усього", len(df))
col2.metric("Топ-1 пара", top20.iloc[0]["symbol"])
col3.metric("24h обсяг топ-1 (USDT)", f"${top20.iloc[0]['quoteVolume']:,.0f}")

fig = px.bar(
    top20,
    x="symbol",
    y="quoteVolume",
    color="priceChangePercent",
    color_continuous_scale="RdYlGn",
    labels={
        "symbol": "Пара",
        "quoteVolume": "Обсяг 24h (USDT)",
        "priceChangePercent": "Зміна ціни, %",
    },
    title="Топ-20 пар Aster DEX за обсягом (24h)",
)
fig.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig, use_container_width=True)

st.dataframe(
    top20[["symbol", "lastPrice", "volume", "quoteVolume", "priceChangePercent"]]
    .rename(columns={
        "symbol": "Пара",
        "lastPrice": "Ціна",
        "volume": "Обсяг (base)",
        "quoteVolume": "Обсяг (USDT)",
        "priceChangePercent": "Зміна, %",
    }),
    use_container_width=True,
)
