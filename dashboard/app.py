import streamlit as st
import pandas as pd
import os
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import plotly.graph_objects as go

st.title("Server Log Analytics")
st.write("Live Component Monitoring Dashboard")
st.divider()

root_dir = os.getcwd()


def resolve_processed_data_path(base_dir):
    candidates = [
        os.path.join(base_dir, "data", "processed_data", "data", "processed_data"),
        os.path.join(base_dir, "data", "processed_data"),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            parquet_dirs = [
                os.path.join(candidate, name)
                for name in os.listdir(candidate)
                if name.endswith(".parquet")
            ]
            if any(parquet_has_data(parquet_dir) for parquet_dir in parquet_dirs):
                return candidate
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return candidates[0]


def parquet_has_data(parquet_dir):
    if not os.path.isdir(parquet_dir):
        return False
    return any(name.startswith("part-") for name in os.listdir(parquet_dir))


req_path = resolve_processed_data_path(root_dir)

st.header("🚨 Anomaly Detection")
st.caption("Isolation Forest model trained on hourly log windows — flags time periods with abnormal log behaviour.")

ANOMALY_FILE = os.path.join(req_path, "4_anomaly_features.parquet")
FEATURES = ["Total_Logs", "CBS_Count", "CSI_Count", "Unique_Templates", "Active_Components", "CSI_Ratio"]

try:
    if not os.path.exists(ANOMALY_FILE):
        st.warning("Anomaly features parquet not found. Run windows_2k_analysis.py first to generate `4_anomaly_features.parquet`.")
    elif not parquet_has_data(ANOMALY_FILE):
        st.warning("Anomaly parquet exists but has no data files. Re-run windows_2k_analysis.py and verify parser/output path configuration.")
    else:
        with st.spinner("Running Isolation Forest..."):
            df_anom = pd.read_parquet(ANOMALY_FILE)
            missing = [c for c in FEATURES if c not in df_anom.columns]
            if missing:
                st.error(f"Anomaly features are missing from parquet: {missing}")
                st.stop()

            df_anom = df_anom.dropna(subset=FEATURES).copy()
            if df_anom.empty:
                st.warning("Anomaly features parquet was loaded but no valid rows were found after cleaning.")
                st.stop()

            if len(df_anom) < 2:
                st.warning("Not enough hourly windows for anomaly detection yet.")
                st.dataframe(df_anom, use_container_width=True)
                st.stop()

            df_anom["Hour_Window"] = pd.to_datetime(df_anom["Hour_Window"])

            # ── Scale features before fitting — Isolation Forest is sensitive to scale ──
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(df_anom[FEATURES])

            # ── Dynamically set contamination based on dataset size ──
            # More windows = more confident about the anomaly rate
            n_windows = len(df_anom)
            if n_windows < 10:
                contamination = 0.1
            elif n_windows < 100:
                contamination = 0.05
            else:
                contamination = 0.03   # 3% for large datasets like 27GB full run

            model = IsolationForest(
                n_estimators=200,        # more trees = more stable results
                contamination=contamination,
                max_samples="auto",
                random_state=42
            )
            df_anom["Anomaly"] = model.fit_predict(X_scaled)
            df_anom["Anomaly_Score"] = model.decision_function(X_scaled)

            # -1 = anomaly, 1 = normal
            df_anom["Is_Anomaly"] = df_anom["Anomaly"] == -1

            # Compute stats for explanations
            vol_mean   = df_anom["Total_Logs"].mean()
            vol_std    = df_anom["Total_Logs"].std()
            tmpl_mean  = df_anom["Unique_Templates"].mean()
            tmpl_std   = df_anom["Unique_Templates"].std()
            csi_mean   = df_anom["CSI_Ratio"].mean()
            csi_std    = df_anom["CSI_Ratio"].std()

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

        # ── Key finding callout ──
        if flagged > 0:
            st.info(
                f"🔍 **Most anomalous window:** {worst_window.strftime('%Y-%m-%d %H:%M')} — "
                f"Isolation Forest confidence score: {df_anom['Anomaly_Score'].min():.4f} "
                f"(more negative = more anomalous)"
            )

        # ── Scatter: Log volume over time, coloured by anomaly ──
        df_anom["Status"] = df_anom["Is_Anomaly"].map({True: "Anomaly 🔴", False: "Normal 🟢"})
        df_anom["dot_size"] = df_anom["CSI_Count"].clip(lower=500)  # minimum dot size

        fig = px.scatter(
            df_anom,
            x="Hour_Window",
            y="Total_Logs",
            color="Status",
            color_discrete_map={"Anomaly 🔴": "#e74c3c", "Normal 🟢": "#2ecc71"},
            size="dot_size",                    # use clipped size
            size_max=30,                        # cap the maximum dot size
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
                # Volume anomalies
                if row["Total_Logs"] > vol_mean + 2 * vol_std:
                    reasons.append(f"log flood ({int(row['Total_Logs'])} logs vs avg {int(vol_mean)})")
                if row["Total_Logs"] < vol_mean - 1.5 * vol_std:
                    reasons.append(f"unusually quiet ({int(row['Total_Logs'])} logs vs avg {int(vol_mean)})")
                # CSI activity anomalies
                if row["CSI_Ratio"] > csi_mean + 2 * csi_std:
                    reasons.append(f"CSI spike ({row['CSI_Ratio']*100:.1f}% vs avg {csi_mean*100:.1f}%)")
                if row["CSI_Ratio"] < csi_mean - 2 * csi_std and row["CSI_Ratio"] < 0.01:
                    reasons.append(f"CBS-only activity (no CSI logs)")
                # Template diversity anomalies
                if row["Unique_Templates"] > tmpl_mean + 2 * tmpl_std:
                    reasons.append(f"unusual event diversity ({int(row['Unique_Templates'])} unique templates vs avg {int(tmpl_mean)})")
                if row["Unique_Templates"] < tmpl_mean - 2 * tmpl_std:
                    reasons.append(f"very low event diversity ({int(row['Unique_Templates'])} unique templates)")
                if not reasons:
                    reasons.append("unusual combination of log metrics across volume, component ratio, and event diversity")
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

            # ── Summary findings ──
            st.subheader("📋 Summary Findings")
            flood_windows   = flagged_df[flagged_df["Total_Logs"] > vol_mean + 2 * vol_std]
            quiet_windows   = flagged_df[flagged_df["Total_Logs"] < vol_mean - 1.5 * vol_std]
            csi_spikes      = flagged_df[flagged_df["CSI_Ratio"] > csi_mean + 2 * csi_std]

            if not flood_windows.empty:
                st.warning(f"⚠️ **{len(flood_windows)} log flood window(s)** detected — system generated abnormally high log volume, possibly indicating a crash loop or update failure.")
            if not quiet_windows.empty:
                st.warning(f"⚠️ **{len(quiet_windows)} silent window(s)** detected — system produced far fewer logs than normal, possibly indicating a hung process or service outage.")
            if not csi_spikes.empty:
                st.warning(f"⚠️ **{len(csi_spikes)} CSI spike window(s)** detected — unusually high Component Servicing Infrastructure activity, possibly indicating a large Windows update or component repair.")
            if flood_windows.empty and quiet_windows.empty and csi_spikes.empty:
                st.info("ℹ️ Flagged windows show subtle multi-feature anomalies not attributable to a single cause.")
        else:
            st.success("✅ No anomalous windows detected. System looks healthy.")

except Exception as e:
    st.error(f"Anomaly Detection Error: {e}")

st.divider()


st.header("📊 Log Analytics")

SKIP_FILES = {"4_anomaly_features.parquet"}

try:
    if os.path.exists(req_path):
        parquet_files = [
            f for f in os.listdir(req_path)
            if f.endswith(".parquet") and f not in SKIP_FILES and parquet_has_data(os.path.join(req_path, f))
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