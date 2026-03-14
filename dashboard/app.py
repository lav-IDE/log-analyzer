import streamlit as st
import pandas as pd

st.title("Server Log Analytics")
st.write("Live Component Monitoring Dashboard")

PARQUET_PATH = "/mnt/d/programs/projects/log_analyzer/data/processed_data/component_summary.parquet"

try:
    with st.spinner('Loading data...'):
        df = pd.read_parquet(PARQUET_PATH)

    st.subheader("Raw Summary Data")
    st.dataframe(df)
   
    st.subheader("Log Volume by Component")
    st.bar_chart(data=df, x="Component", y="Total_Logs" )
    

except Exception as e:
    st.error(f"Error: {e}")