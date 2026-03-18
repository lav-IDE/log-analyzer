import streamlit as st
import pandas as pd
import os

st.title("Server Log Analytics")
st.write("Live Component Monitoring Dashboard")
st.divider()


dir = os.getcwd()

try:
    
    if os.path.exists(dir):
        req_path = os.path.join(dir, "data", "processed_data")
        parquet_files = [f for f in os.listdir(req_path) if f.endswith(".parquet")]
        parquet_files.sort()
    
    for file in parquet_files:
        clean_name = file.replace('.parquet', "").replace("_", ") ", 1).replace("_", " ").title()
        st.subheader(clean_name)
        
        with st.spinner("Loading..."):
            df = pd.read_parquet(os.path.join(req_path, file))
            col1, col2 = st.columns([1,2])
        
        with col1: 
            st.dataframe(df)
        
        with col2:
            if len(df.columns)>=2:
                st.bar_chart(data=df, x=df.columns[0], y=df.columns[1])
                
        st.divider()
    

except Exception as e:
    st.error(f"Error: {e}")