import streamlit as st
import pandas as pd
from io import BytesIO
from minio import Minio
import plotly.express as px
import numpy as np


# ---------------------------
# Configuração básica da página
# ---------------------------

st.set_page_config(
    page_title="XAUUSD Gold Analytics",
    layout="wide",
)

st.title("📊 XAUUSD Gold Analytics")


# ---------------------------
# Função para carregar dados Gold do MinIO
# ---------------------------

BUCKET_NAME = "xau-lake"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"


@st.cache_data(show_spinner="Carregando dados do MinIO...")
def load_gold(timeframe: str = "1d") -> pd.DataFrame:
    """
    Lê o Parquet Gold de um determinado timeframe diretamente do MinIO.
    """
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    object_key = f"gold/timeframe={timeframe}/XAU_{timeframe}_features.parquet"

    response = client.get_object(BUCKET_NAME, object_key)
    data_bytes = response.read()
    response.close()
    response.release_conn()

    buffer = BytesIO(data_bytes)
    df = pd.read_parquet(buffer)

    # Garantir tipos
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    return df


# ---------------------------
# Sidebar - filtros
# ---------------------------

st.sidebar.header("Filtros")

# Por enquanto só 1d, mas já deixamos a estrutura pronta
timeframe = st.sidebar.selectbox(
    "Timeframe",
    options=["1d"],
    index=0,
)

df = load_gold(timeframe)

min_date = df["timestamp"].min().date()
max_date = df["timestamp"].max().date()

date_range = st.sidebar.date_input(
    "Intervalo de datas",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Garantir que sempre temos um intervalo (tupla)
if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date = date_range
    end_date = date_range

mask = (df["timestamp"].dt.date >= start_date) & (df["timestamp"].dt.date <= end_date)
df_period = df.loc[mask].copy()


# ---------------------------
# KPIs principais
# ---------------------------
tab_overview, tab_risk = st.tabs(["📈 Visão Geral", "⚠️ Risco"])

with tab_overview:
    st.markdown(
        "### Visão Geral\n"
        "Análise da evolução do preço do ouro (XAUUSD) e do retorno acumulado "
        "no período selecionado."
    )

    # ---------------------------
    # KPIs principais
    # ---------------------------

    col1, col2, col3, col4 = st.columns(4)

    if not df_period.empty:
        cum_ret = df_period["cum_return"].iloc[-1]
        close_min = df_period["close"].min()
        close_max = df_period["close"].max()

        returns = df_period["return"].dropna()
        daily_vol = returns.std()
        annual_vol = daily_vol * np.sqrt(252)

        with col1:
            st.metric(
                "Retorno acumulado no período",
                f"{(1 + cum_ret):.2f}x",
                help="(1 + cum_return). Ex.: 2.50x = +150% em relação ao início do período.",
            )

        with col2:
            st.metric(
                "Preço mínimo (USD)",
                f"{close_min:,.2f}",
            )

        with col3:
            st.metric(
                "Preço máximo (USD)",
                f"{close_max:,.2f}",
            )

        with col4:
            st.metric(
                "Volatilidade anualizada",
                f"{annual_vol * 100:.2f}%",
                help="Desvio padrão dos retornos diários, anualizado (≈ std * sqrt(252)).",
            )
    else:
        st.warning("Nenhum dado no intervalo selecionado.")
        st.stop()

    st.markdown("---")

    # ---------------------------
    # Gráfico 1 - Retorno acumulado
    # ---------------------------

    st.subheader("Retorno acumulado XAUUSD")

    fig_cum = px.line(
        df_period,
        x="timestamp",
        y="cum_return",
        labels={"timestamp": "Data", "cum_return": "Retorno acumulado"},
        title=f"Retorno acumulado XAUUSD ({timeframe})",
    )

    st.plotly_chart(fig_cum, use_container_width=True)

    # ---------------------------
    # Gráfico 2 - Preço + Médias Móveis
    # ---------------------------

    st.subheader("Preço e Médias Móveis")

    fig_price = px.line(
        df_period,
        x="timestamp",
        y=["close", "ma20", "ma50"],
        labels={"timestamp": "Data", "value": "Preço (USD)", "variable": "Série"},
        title=f"Preço de fechamento e MAs ({timeframe})",
    )

    st.plotly_chart(fig_price, use_container_width=True)

# ---------------------------
# Aba Risco com distribuição de retornos
# ---------------------------

with tab_risk:
    st.markdown(
        "### Risco\n"
        "Distribuição dos retornos diários do período selecionado."
    )

    returns = df_period["return"].dropna()

    if returns.empty:
        st.warning("Não há retornos suficientes no intervalo selecionado.")
    else:
        fig_hist = px.histogram(
            returns,
            nbins=50,
            marginal="box",  # opcional: acrescenta boxplot em cima
            histnorm="probability",  # frequência relativa (0 a 1)
        )
        fig_hist.update_layout(
            title=f"Distribuição dos retornos ({timeframe})",
            xaxis_title="Retorno diário",
            yaxis_title="Frequência relativa",
        )
        st.plotly_chart(fig_hist, use_container_width=True)