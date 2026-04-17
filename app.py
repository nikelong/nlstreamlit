import streamlit as st
import pandas as pd
import plotly.express as px

st.title("Onchain Analytics")

# Тестові дані — топ пари за fee (M1)
data = {
    "pair": ["BTC-USD", "ETH-USD", "SOL-USD", "ARB-USD", "DOGE-USD"],
    "fee_paid": [142000, 98000, 45000, 21000, 9000],
    "trades": [3200, 2800, 1500, 800, 400],
}
df = pd.DataFrame(data)

st.subheader("Топ пари за fee (M1)")
fig = px.bar(df, x="pair", y="fee_paid", color="fee_paid",
             color_continuous_scale="Blues")
st.plotly_chart(fig, use_container_width=True)

st.subheader("Таблиця")
st.dataframe(df)
