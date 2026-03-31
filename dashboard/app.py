import streamlit as st
import pandas as pd
import os
import numpy as np
from sklearn.ensemble import IsolationForest
import plotly.express as px
import plotly.graph_objects as go

st.title("Server Log Analytics")
st.write("Live Component Monitoring Dashboard")
st.divider()

dir = os.getcwd()
req_path = os.path.join(dir, "data", "processed_data")

# ─────────────────────────────────────────────
# SECTION 1: ANOMALY DETECTION (shown at top)
# ─────────────────────────────────────────────

st.header("Anomaly Detection")
st.caption("Isolation Forest model trained on hourly log windows — flags time periods with abnormal log behaviour.")

ANOMALY_FILE = os.path.join(req_path, "4_anomaly_features.parquet")
FEATURES = ["Total_Logs", "CBS_Count", "CSI_Count", "Unique_Templates", "Active_Components", "CSI_Ratio"]

try:
    if not os.path.exists(ANOMALY_FILE):
        st.warning("Anomaly features parquet not found. Run analysis.py first to generate `4_anomaly_features.parquet`.")
    else:
        with st.spinner("Running Isolation Forest..."):
            df_anom = pd.read_parquet(ANOMALY_FILE)
            df_anom = df_anom.dropna(subset=FEATURES).copy()
            df_anom["Hour_Window"] = pd.to_datetime(df_anom["Hour_Window"])

            # Train Isolation Forest
            # contamination=0.05 → expects ~5% of windows to be anomalous
            model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
            df_anom["Anomaly"] = model.fit_predict(df_anom[FEATURES])
            df_anom["Anomaly_Score"] = model.decision_function(df_anom[FEATURES])

            # -1 = anomaly, 1 = normal  (sklearn convention)
            df_anom["Is_Anomaly"] = df_anom["Anomaly"] == -1

            # Compute volume stats for plain-English explanations
            vol_mean = df_anom["Total_Logs"].mean()
            vol_std  = df_anom["Total_Logs"].std()

        # ── KPI row ──
        total_windows  = len(df_anom)
        flagged        = df_anom["Is_Anomaly"].sum()
        worst_window   = df_anom.loc[df_anom["Anomaly_Score"].idxmin(), "Hour_Window"]
        peak_csi_ratio = df_anom.loc[df_anom["Is_Anomaly"], "CSI_Ratio"].max() if flagged else 0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Hour Windows", total_windows)
        k2.metric("Anomalous Windows",  int(flagged))
        k3.metric("Anomaly Rate",       f"{flagged/total_windows*100:.1f}%")
        k4.metric("Peak CSI Ratio",     f"{peak_csi_ratio*100:.1f}%")

        st.markdown("")

        # ── Scatter: Log volume over time, coloured by anomaly ──
        df_anom["Status"] = df_anom["Is_Anomaly"].map({True: "Anomaly 🔴", False: "Normal 🟢"})
        fig = px.scatter(
            df_anom,
            x="Hour_Window",
            y="Total_Logs",
            color="Status",
            color_discrete_map={"Anomaly 🔴": "#e74c3c", "Normal 🟢": "#2ecc71"},
            size="CSI_Count",
            hover_data=["CSI_Ratio", "Unique_Templates", "Anomaly_Score"],
            title="Log Volume Over Time — Anomalous Windows Highlighted",
            labels={"Hour_Window": "Time Window", "Total_Logs": "Total Logs in Window"}
        )
        fig.update_layout(legend_title_text="Window Status")
        st.plotly_chart(fig, use_container_width=True)

        # ── CSI Ratio timeline with flagged markers ──
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df_anom["Hour_Window"], y=df_anom["CSI_Ratio"],
            mode="lines", name="CSI Ratio",
            line=dict(color="#3498db", width=1.5)
        ))
        anomalies_only = df_anom[df_anom["Is_Anomaly"]]
        fig2.add_trace(go.Scatter(
            x=anomalies_only["Hour_Window"], y=anomalies_only["CSI_Ratio"],
            mode="markers", name="Flagged Window",
            marker=dict(color="#e74c3c", size=10, symbol="x")
        ))
        fig2.update_layout(
            title="CSI Ratio Timeline — Flagged Windows Marked",
            xaxis_title="Time Window",
            yaxis_title="CSI Ratio",
            legend_title_text="Series"
        )
        st.plotly_chart(fig2, use_container_width=True)

        # ── Flagged windows table with plain-English explanations ──
        if flagged > 0:
            st.subheader("🔍 Flagged Windows — Detail")

            def explain(row):
                reasons = []
                if row["Total_Logs"] > vol_mean + 2 * vol_std:
                    reasons.append(f"log flood ({int(row['Total_Logs'])} logs vs avg {int(vol_mean)})")
                if row["Total_Logs"] < vol_mean - 1.5 * vol_std:
                    reasons.append(f"unusually quiet ({int(row['Total_Logs'])} logs)")
                if row["CSI_Ratio"] > 0.5:
                    reasons.append(f"unusually high CSI activity ({row['CSI_Ratio']*100:.1f}%)")
                if row["Unique_Templates"] > df_anom["Unique_Templates"].mean() + df_anom["Unique_Templates"].std():
                    reasons.append(f"many unique event types ({int(row['Unique_Templates'])})")
                if not reasons:
                    reasons.append("unusual combination of log metrics")
                return ", ".join(reasons)

            flagged_df = df_anom[df_anom["Is_Anomaly"]].copy()
            flagged_df["Why Flagged"] = flagged_df.apply(explain, axis=1)
            flagged_df["Anomaly Score"] = flagged_df["Anomaly_Score"].round(4)
            flagged_df["Hour_Window"] = flagged_df["Hour_Window"].astype(str)

            display_cols = ["Hour_Window", "Total_Logs", "CBS_Count", "CSI_Count",
                            "CSI_Ratio", "Unique_Templates", "Anomaly Score", "Why Flagged"]
            st.dataframe(
                flagged_df[display_cols].sort_values("Anomaly Score").reset_index(drop=True),
                use_container_width=True
            )
        else:
            st.success("No anomalous windows detected. System looks healthy.")

except Exception as e:
    st.error(f"Anomaly Detection Error: {e}")

st.divider()

# ─────────────────────────────────────────────
# SECTION 2: EXISTING DASHBOARD (unchanged)
# ─────────────────────────────────────────────

st.header("📊 Log Analytics")

SKIP_FILES = {"4_anomaly_features.parquet"}

try:
    if os.path.exists(req_path):
        parquet_files = [
            f for f in os.listdir(req_path)
            if f.endswith(".parquet") and f not in SKIP_FILES
        ]
        parquet_files.sort()

    for file in parquet_files:
        clean_name = file.replace('.parquet', "").replace("_", ") ", 1).replace("_", " ").title()
        st.subheader(clean_name)

        with st.spinner("Loading..."):
            df = pd.read_parquet(os.path.join(req_path, file))
            col1, col2 = st.columns([1, 2])

        with col1:
            st.dataframe(df)

        with col2:
            if len(df.columns) >= 2:
                st.bar_chart(data=df, x=df.columns[0], y=df.columns[1])

        st.divider()

except Exception as e:
    st.error(f"Error: {e}")