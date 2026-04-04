import os

import altair as alt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


st.set_page_config(page_title="Log Files Analysis", layout="wide")


def parquet_has_data(parquet_dir):
    if not os.path.isdir(parquet_dir):
        return False
    return any(name.startswith("part-") for name in os.listdir(parquet_dir))


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


@st.cache_data(show_spinner=False)
def load_parquet_folder(base_path, folder_name):
    folder_path = os.path.join(base_path, folder_name)
    if not os.path.exists(folder_path) or not parquet_has_data(folder_path):
        return pd.DataFrame()
    return pd.read_parquet(folder_path)


root_dir = os.getcwd()
req_path = resolve_processed_data_path(root_dir)

component_df = load_parquet_folder(req_path, "1_component_volume.parquet")
severity_df = load_parquet_folder(req_path, "2_severity_levels.parquet")
actions_df = load_parquet_folder(req_path, "3_most_frequent_actions.parquet")
anom_df = load_parquet_folder(req_path, "4_anomaly_features.parquet")

st.title("Windows Log Files' Analysis")
st.caption(
    "Analyze 27GB of Windows logs through a PySpark-powered dashboard. "
    "This anomaly detection engine uses Isolation Forest to isolate critical system errors from millions of routine Info events."
)
st.markdown(
    """
### Operational Analysis Report
This dashboard presents a structured analysis of server log behavior, component activity, anomaly risk, and cluster-processing performance.

The report is organized from high-level component comparison to detailed risk and execution efficiency.
Use the controls in the Component Landscape section to adjust component selection and benchmark context.
"""
)
st.markdown(
    """
### Platform Methodology: Docker, Hadoop, and Spark
- Docker provides the reproducible runtime for the data platform, including service isolation and consistent dependency execution across environments.
- Hadoop supplies the distributed storage and batch-processing foundation for large log volumes, allowing data to be partitioned and processed reliably at scale.
- Spark performs high-throughput transformations and aggregations over raw logs, producing parquet outputs consumed by this dashboard.

In short, Docker standardizes deployment, Hadoop organizes distributed data execution, and Spark accelerates analytics that feed the reporting layer.
"""
)
st.divider()

if component_df.empty:
    st.error("Component volume data is missing. Run windows_2k_analysis.py first.")
    st.stop()

component_df = component_df.copy()
component_df["Component"] = component_df["Component"].fillna("").replace("", "Unlabeled")
component_df = component_df.sort_values("Total_Logs", ascending=False).reset_index(drop=True)

total_logs_all = component_df["Total_Logs"].sum()
component_df["SharePct"] = (component_df["Total_Logs"] / total_logs_all) * 100
component_df["Rank"] = component_df["Total_Logs"].rank(method="dense", ascending=False).astype(int)

all_components = component_df["Component"].tolist()
default_components = all_components[: min(6, len(all_components))]

st.markdown("## 1) Component Landscape")
st.markdown(
    """
This section compares selected components. It highlights relative workload distribution and identifies which component dominates log generation.

Interpretation guide:
- Snapshot cards summarize each component's absolute volume and gap versus the selected benchmark.
- Table and charts provide rank, share, and proportional differences.
"""
)

control_col_1, control_col_2, control_col_3 = st.columns([2, 1, 1])
with control_col_1:
    selected_components = st.multiselect(
        "Select Components",
        options=all_components,
        default=default_components,
    )

if not selected_components:
    st.warning("Pick at least one component in this section to continue.")
    st.stop()

with control_col_2:
    benchmark_component = st.selectbox("Benchmark Component", options=selected_components, index=0)
with control_col_3:
    view_mode = st.radio("Comparison View", ["Absolute", "Relative"], horizontal=True)

comparison_df = component_df[component_df["Component"].isin(selected_components)].copy()
benchmark_logs = comparison_df.loc[comparison_df["Component"] == benchmark_component, "Total_Logs"].iloc[0]
comparison_df["Vs_Benchmark"] = (comparison_df["Total_Logs"] / benchmark_logs).round(3)
comparison_df["RelativeGapPct"] = ((comparison_df["Total_Logs"] - benchmark_logs) / benchmark_logs * 100).round(2)

st.subheader("Component Snapshot Grid")
grid_cols = st.columns(3)
for idx, row in comparison_df.reset_index(drop=True).iterrows():
    card_col = grid_cols[idx % 3]
    benchmark_badge = " (Benchmark)" if row["Component"] == benchmark_component else ""
    delta_text = f"{row['RelativeGapPct']:+.1f}% vs {benchmark_component}"
    card_col.metric(
        f"{row['Component']}{benchmark_badge}",
        f"{int(row['Total_Logs']):,}",
        delta=delta_text,
        help=f"Rank #{row['Rank']} • Share {row['SharePct']:.2f}%",
    )
st.caption("Each card shows total logs and relative distance from the benchmark component.")

st.subheader("Component Comparison Table")
st.caption("This comparison table is suitable for direct reporting and export of ranked component metrics.")
display_df = comparison_df[["Component", "Total_Logs", "SharePct", "Rank", "Vs_Benchmark", "RelativeGapPct"]].copy()
display_df = display_df.sort_values("Total_Logs", ascending=False)
st.dataframe(
    display_df.rename(
        columns={
            "SharePct": "Share (%)",
            "Vs_Benchmark": "Multiple vs Benchmark",
            "RelativeGapPct": "Gap vs Benchmark (%)",
        }
    ),
    use_container_width=True,
)

chart_left, chart_right = st.columns(2)

with chart_left:
    st.subheader("Component Bar Grid")
    st.caption("Use this chart to compare selected components by either absolute volume or relative share.")
    y_col = "Total_Logs" if view_mode == "Absolute" else "SharePct"
    y_title = "Total Logs" if view_mode == "Absolute" else "Share (%)"
    fig_bar = px.bar(
        comparison_df.sort_values(y_col, ascending=False),
        x="Component",
        y=y_col,
        color="Component",
        title=f"{y_title} by Component",
        text_auto=".2s" if view_mode == "Absolute" else ".2f",
    )
    fig_bar.update_layout(showlegend=False, xaxis_title="Component", yaxis_title=y_title)
    st.plotly_chart(fig_bar, use_container_width=True)

with chart_right:
    st.subheader("Component Share")
    st.caption("The donut view emphasizes proportional dominance among selected components.")
    fig_pie = px.pie(
        comparison_df,
        names="Component",
        values="Total_Logs",
        hole=0.45,
        title="Share of Selected Components",
    )
    st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("## 2) Time-Series Behavior")
st.markdown(
    """
This section tracks hourly component movement over time using indexed values.
Indexing each series to a base of 100 allows shape-based comparison even when raw magnitudes differ significantly.
"""
)
st.subheader("Hourly Component Trend (Normalized)")
if anom_df.empty:
    st.info("Anomaly feature data not found, so hourly trend and anomaly scoring are hidden.")
else:
    trend_df = anom_df.copy()
    trend_df["Hour_Window"] = pd.to_datetime(trend_df["Hour_Window"])

    component_to_col = {
        "CBS": "CBS_Count",
        "CSI": "CSI_Count",
    }

    figure = go.Figure()
    plotted = 0
    for component_name in selected_components:
        col = component_to_col.get(component_name)
        if not col or col not in trend_df.columns:
            continue
        base = trend_df[col].replace(0, pd.NA).iloc[0]
        if pd.isna(base) or base == 0:
            continue
        indexed = (trend_df[col] / base) * 100
        figure.add_trace(
            go.Scatter(
                x=trend_df["Hour_Window"],
                y=indexed,
                mode="lines",
                name=component_name,
            )
        )
        plotted += 1

    if plotted == 0:
        st.info("No hourly component series available for the selected components (supported: CBS, CSI).")
    else:
        figure.update_layout(
            title="Indexed Trend (Base = 100 at first timestamp)",
            xaxis_title="Hour Window",
            yaxis_title="Indexed Log Count",
            legend_title_text="Component",
        )
        st.plotly_chart(figure, use_container_width=True)
        st.caption("Steeper slopes indicate faster changes in activity; crossing lines suggest shifting relative behavior.")

st.markdown("## 3) Risk and Anomaly Assessment")
st.markdown(
    """
This section applies Isolation Forest over engineered hourly features to flag unusual operating windows.
Anomaly scores become more negative as windows appear less consistent with baseline behavior.
"""
)
st.subheader("Risk Panel")
if anom_df.empty:
    st.info("Anomaly panel unavailable until 4_anomaly_features.parquet is generated.")
else:
    FEATURES = ["Total_Logs", "CBS_Count", "CSI_Count", "Unique_Templates", "Active_Components", "CSI_Ratio"]
    model_df = anom_df.dropna(subset=[f for f in FEATURES if f in anom_df.columns]).copy()
    if len(model_df) < 2 or any(f not in model_df.columns for f in FEATURES):
        st.warning("Insufficient anomaly feature rows or columns for scoring.")
    else:
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(model_df[FEATURES])
        contamination = 0.1 if len(model_df) < 10 else 0.05 if len(model_df) < 100 else 0.03

        model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            random_state=42,
        )
        model.fit(X_scaled)
        model_df["Anomaly_Score"] = model.decision_function(X_scaled)
        model_df["Is_Anomaly"] = model.predict(X_scaled) == -1
        model_df["Hour_Window"] = pd.to_datetime(model_df["Hour_Window"])

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Windows", len(model_df))
        c2.metric("Flagged Windows", int(model_df["Is_Anomaly"].sum()))
        c3.metric("Anomaly Rate", f"{model_df['Is_Anomaly'].mean() * 100:.1f}%")

        model_df["Status"] = model_df["Is_Anomaly"].map({True: "Anomaly 🔴", False: "Normal 🟢"})
        model_df["dot_size"] = model_df["CSI_Count"].clip(lower=500)

        st.markdown(
            """
**What this scatter plot tells you:**
Each point represents one hourly log window. The chart shows **when** activity occurred (x-axis) and **how much** activity occurred (y-axis).

- **Green points (Normal):** windows that match usual system behavior.
- **Red points (Anomaly):** windows that differ from normal patterns and may indicate unusual or risky activity.
- **Larger points:** hours with higher CSI event volume.

**Why high CSI volume can be a concern:**
CSI (Component Servicing Infrastructure) activity often increases during updates, repairs, or component state changes. A sudden spike is not always a failure, but it can signal heavy servicing pressure on the system. In practical terms, high CSI bursts may correlate with update instability, repeated repair attempts, or abnormal component churn, so those windows are good candidates for closer review.
"""
        )

        with st.expander("How to Read the Anomaly Scatter Plot (Beginner Guide)", expanded=True):
            st.markdown(
                """
This chart is designed to answer one simple question: **When did log behavior look unusual?**

Use it in this order:
1. **Start with color**:
- Green points are normal hourly windows.
- Red points are windows flagged as unusual by the model.

2. **Read the axes**:
- **X-axis (Time Window):** when the activity happened.
- **Y-axis (Total Logs):** how much activity happened in that hour.

3. **Check point size**:
- Larger circles mean more CSI-related events in that hour.
- A large red circle is usually worth immediate review.

4. **Prioritize what to investigate first**:
- Red points that are far away from the green cluster.
- Red points during known incident times.
- Red points with very high total logs or unusually large marker size.

Plain-language takeaway:
If you see isolated red points or sudden bursts of large red circles, those are likely the best starting points for root-cause investigation.
"""
            )

        simplified_anomaly_view = st.toggle(
            "Simplified anomaly view (show only flagged windows)",
            value=False,
            help="When enabled, the chart focuses only on anomalous windows and shows the top 5 most critical rows.",
        )

        scatter_df = model_df.copy()
        if simplified_anomaly_view:
            scatter_df = model_df[model_df["Is_Anomaly"]].copy()

        if scatter_df.empty:
            st.info("No flagged windows are available for simplified view in the current dataset.")
        else:
            if simplified_anomaly_view:
                fig_scatter = px.scatter(
                    scatter_df,
                    x="Hour_Window",
                    y="Total_Logs",
                    size="dot_size",
                    size_max=30,
                    color_discrete_sequence=["#e74c3c"],
                    hover_data=["CSI_Ratio", "Unique_Templates", "Anomaly_Score"],
                    title="Flagged Windows Only — Simplified View",
                    labels={"Hour_Window": "Time Window", "Total_Logs": "Total Logs in Window"},
                )
                fig_scatter.update_traces(marker=dict(color="#e74c3c"), name="Anomaly")
            else:
                fig_scatter = px.scatter(
                    scatter_df,
                    x="Hour_Window",
                    y="Total_Logs",
                    color="Status",
                    color_discrete_map={"Anomaly 🔴": "#e74c3c", "Normal 🟢": "#2ecc71"},
                    size="dot_size",
                    size_max=30,
                    hover_data=["CSI_Ratio", "Unique_Templates", "Anomaly_Score"],
                    title="Log Volume Over Time — Anomalous Windows Highlighted",
                    labels={"Hour_Window": "Time Window", "Total_Logs": "Total Logs in Window"},
                )

            fig_scatter.update_layout(legend_title_text="Window Status")
            st.plotly_chart(fig_scatter, use_container_width=True)
            st.caption("Highlighted red points represent flagged windows; larger markers indicate higher CSI event volume.")

        flagged = model_df[model_df["Is_Anomaly"]].copy()
        if flagged.empty:
            st.success("No anomalous hourly windows detected.")
        else:
            if simplified_anomaly_view:
                st.caption("Top 5 most critical anomalous windows (lowest anomaly score first).")
                flagged = flagged.sort_values("Anomaly_Score").head(5)
            else:
                st.caption("Top anomalous windows are ranked by anomaly score for operational triage.")
            st.dataframe(
                flagged[["Hour_Window", "Total_Logs", "CBS_Count", "CSI_Count", "CSI_Ratio", "Anomaly_Score"]]
                .sort_values("Anomaly_Score")
                .head(20 if not simplified_anomaly_view else 5),
                use_container_width=True,
            )

if not severity_df.empty:
    st.divider()
    st.markdown("## 4) Severity Distribution")
    st.markdown(
        """
This view focuses on signal quality by separating high-volume informational logs from low-volume error logs.
The metric row highlights error proportion so critical issues remain visible even when total volume is dominated by info events.
"""
    )
    st.subheader("Severity Mix")

    sev_df = severity_df.copy()
    sev_df["Level"] = sev_df["Level"].astype(str)

    info_count = int(sev_df.loc[sev_df["Level"].str.lower() == "info", "Total_Logs"].sum())
    error_count = int(sev_df.loc[sev_df["Level"].str.lower() == "error", "Total_Logs"].sum())
    total_count = int(sev_df["Total_Logs"].sum())
    error_pct = (error_count / total_count * 100) if total_count else 0.0

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Logs", f"{total_count:,}")
    m2.metric("Error Logs", f"{error_count:,}")
    m3.metric("Error %", f"{error_pct:.3f}%", delta=f"{error_count:,} of {total_count:,}")

    palette = ["#29B5E8", "#FF4B4B"]

    donut = (
        alt.Chart(sev_df)
        .mark_arc(innerRadius=80, outerRadius=130)
        .encode(
            theta=alt.Theta("Total_Logs:Q", stack=True),
            color=alt.Color(
                "Level:N",
                scale=alt.Scale(domain=["Info", "Error"], range=palette),
                legend=alt.Legend(title="Level"),
            ),
            tooltip=[
                alt.Tooltip("Level:N", title="Level"),
                alt.Tooltip("Total_Logs:Q", title="Total Logs", format=","),
            ],
        )
    )

    center_text_df = pd.DataFrame(
        {
            "line1": ["Total Logs"],
            "line2": [f"{total_count:,}"],
        }
    )

    center_line_1 = alt.Chart(center_text_df).mark_text(
        align="center",
        baseline="middle",
        dy=-10,
        fontSize=16,
        fontWeight="bold",
    ).encode(text="line1:N")

    center_line_2 = alt.Chart(center_text_df).mark_text(
        align="center",
        baseline="middle",
        dy=14,
        fontSize=18,
    ).encode(text="line2:N")

    donut_chart = (donut + center_line_1 + center_line_2).properties(height=340)
    st.altair_chart(donut_chart, use_container_width=True)
    st.caption("Donut center displays total volume; color segmentation shows Info vs Error contribution.")

if not actions_df.empty:
    st.markdown("## 5) Event Template Concentration")
    st.markdown(
        """
This section identifies which event templates consume the most processing activity.
Buttons are intentionally compact for readability; hover to inspect full template text.
"""
    )
    st.subheader("Top Event Templates")
    top_actions = actions_df.sort_values("Actions", ascending=False).head(8).reset_index(drop=True)

    col_evt, col_count = st.columns([3, 1])
    with col_evt:
        st.caption("Hover each event chip to see the full template text.")
    with col_count:
        st.caption("Action count")

    for idx, row in top_actions.iterrows():
        event_name = str(row["EventTemplate"])
        short_name = event_name if len(event_name) <= 80 else f"{event_name[:77]}..."
        row_col_1, row_col_2 = st.columns([3, 1])
        with row_col_1:
            st.button(short_name, key=f"event_btn_{idx}", help=event_name, use_container_width=True)
        with row_col_2:
            st.metric("Actions", f"{int(row['Actions']):,}")

st.divider()
st.markdown("## 6) Cluster Efficiency Comparison")
st.markdown(
    """
This section compares processing duration between one-worker and two-worker cluster configurations across each pipeline stage.
It provides both trend visualization and per-stage speedup for performance reporting.
"""
)
st.subheader("Worker Node Performance Comparison")

perf_df = pd.DataFrame(
    [
        {"Process": "Component Volume", "Worker Setup": "1 Worker Node", "Seconds": 147.87},
        {"Process": "Severity Levels", "Worker Setup": "1 Worker Node", "Seconds": 98.76},
        {"Process": "Top Event Templates", "Worker Setup": "1 Worker Node", "Seconds": 736.07},
        {"Process": "Anomaly Features", "Worker Setup": "1 Worker Node", "Seconds": 1079.15},
        {"Process": "Component Volume", "Worker Setup": "2 Worker Nodes", "Seconds": 71.59},
        {"Process": "Severity Levels", "Worker Setup": "2 Worker Nodes", "Seconds": 44.95},
        {"Process": "Top Event Templates", "Worker Setup": "2 Worker Nodes", "Seconds": 300.09},
        {"Process": "Anomaly Features", "Worker Setup": "2 Worker Nodes", "Seconds": 435.62},
    ]
)

process_order = ["Component Volume", "Severity Levels", "Top Event Templates", "Anomaly Features"]
perf_df["Process"] = pd.Categorical(perf_df["Process"], categories=process_order, ordered=True)
perf_df = perf_df.sort_values(["Worker Setup", "Process"])

fig_perf = px.line(
    perf_df,
    x="Process",
    y="Seconds",
    color="Worker Setup",
    markers=True,
    title="Processing Time by Stage: 1 Worker vs 2 Workers",
    color_discrete_map={"1 Worker Node": "#FF4B4B", "2 Worker Nodes": "#29B5E8"},
)
fig_perf.update_layout(xaxis_title="Processing Stage", yaxis_title="Time (seconds)")
st.plotly_chart(fig_perf, use_container_width=True)
st.caption("Lower values are better. The blue line indicates the two-worker profile and expected acceleration.")

speedup_df = (
    perf_df.pivot(index="Process", columns="Worker Setup", values="Seconds")
    .reset_index()
)
speedup_df["Speedup (1W / 2W)"] = (speedup_df["1 Worker Node"] / speedup_df["2 Worker Nodes"]).round(2)

st.dataframe(
    speedup_df.rename(
        columns={
            "1 Worker Node": "1 Worker Time (s)",
            "2 Worker Nodes": "2 Worker Time (s)",
        }
    ),
    use_container_width=True,
)
st.caption("Speedup values above 1.0 indicate performance gain from using two worker nodes.")

st.markdown(
    """
### Report Conclusion
Across component distribution, anomaly scoring, and execution benchmarks, the dataset shows clear concentration in selected components and measurable runtime gains when scaling from one to two worker nodes.
This report layout is designed for both operational monitoring and stakeholder-ready performance review.
"""
)

st.markdown(
    """
### Practical Value for End Users
Even without a deep engineering background, a normal user can use this project to understand whether a system is healthy, where log activity is concentrated, and when unusual behavior starts.

How users benefit:
- Faster issue visibility: Error rates and anomaly windows are surfaced quickly, so users can detect problems before they escalate.
- Clear prioritization: The dashboard highlights which components and event templates drive most activity, making troubleshooting more focused.
- Better operational decisions: Time-based trends reveal when activity spikes, drops, or shifts, helping users correlate incidents with updates or outages.
- Evidence-based scaling: Runtime comparisons between one-worker and two-worker execution show the practical performance gain from adding compute resources.

Key insights gathered by this project:
- Volume concentration: A small set of components typically contributes most of the total log volume.
- Signal imbalance: Error logs are often numerically small versus informational logs, but still visible through dedicated severity metrics.
- Behavioral anomalies: Specific hourly windows can be flagged as statistically unusual based on multi-feature patterns.
- Pipeline efficiency: Distributed processing with additional workers materially reduces end-to-end processing time across all major stages.
"""
)

st.markdown(
    """
### Dataset Citation
If you use the Windows log dataset in this project, please cite:

Jieming Zhu, Shilin He, Pinjia He, Jinyang Liu, Michael R. Lyu.
*Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics.*
IEEE ISSRE, 2023. https://github.com/logpai/loghub
"""
)