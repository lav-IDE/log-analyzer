# Log Analyzer

A distributed log analysis system built with Apache Spark and Apache Hadoop to process large-scale Windows system logs, detect anomalies, and present operational insights through an interactive Streamlit report dashboard.

---

## What This Does

System logs contain critical reliability signals, but at production scale they are too large for manual analysis. This project provides an end-to-end distributed pipeline that:

- Ingests and processes real Windows logs using Hadoop/HDFS.
- Runs distributed aggregations and feature generation with Spark.
- Detects anomalous operating windows using Isolation Forest.
- Publishes a professional, report-style dashboard for analysis and communication.

---

## Recent Dashboard Additions

The dashboard has been expanded significantly and now includes:

- Component-focused comparison controls embedded directly in the Component Landscape section.
- Risk and anomaly panel with:
	- Normal vs anomaly scatter plot
	- Beginner-friendly interpretation text
	- Red/green point meaning and CSI volume explanation
	- Simplified mode that shows flagged windows only
	- Top critical anomaly windows table
- Severity distribution redesign using an Altair donut chart:
	- Info vs Error palette
	- Center text showing total logs
	- Error percentage metric row for quick visibility
- Top event templates with compact button labels and hover tooltips for full event names.
- Cluster efficiency section with 1-worker vs 2-worker line chart and stage-wise speedup table.
- In-report documentation for Docker, Hadoop, and Spark methodology.
- End-user benefit summary and in-dashboard dataset citation.

---

## Dataset

This project uses the Windows log dataset from [logpai/loghub](https://github.com/logpai/loghub), a benchmark collection used in log analytics research.

- Source: Real Windows 7 CBS logs from `C:\Windows\Logs\CBS`
- Full dataset size: 27+ GB across 226+ days
- Development sample: `Windows_2k.log` (2,000 structured entries)
- Log format: `Date Time Level Component Content`

The `Windows_2k` sample is used to validate the full pipeline before scaling to the complete dataset.

---

## Project Structure

```text
log_analyzer/
|- log_generator/        # Synthetic log generator (pipeline testing)
|  \- log_generator.py
|- spark/                # Spark analysis pipeline
|  \- windows_2k_analysis.py
|- dashboard/            # Streamlit report dashboard
|  \- app.py
|- data/
|  |- raw_logs/          # Windows_2k.log and generated logs
|  \- processed_data/    # Spark outputs consumed by dashboard
|- hadoop-project/       # Dockerized Hadoop+Spark cluster definitions
|  |- docker-compose.yml
|  |- dockerfile
|  |- entrypoint.sh
|  |- hadoop.env.example
|  \- hadoop/etc/hadoop/ # Hadoop XML configs used by containers
|- scripts/              # Utility scripts (start/stop/upload cluster)
|- requirements.txt
\- README.md
```

---

## Pipeline Overview

### 1. Log Ingestion

Windows CBS logs are loaded into HDFS for distributed processing. The `Windows_2k.log` sample is used during development.

Example log format:

```text
Date       Time     Level   Component   Content
2016-09-28 04:30:30 Info    CBS         Starting TrustedInstaller initialization.
2016-09-28 04:30:30 Warning CBS         Failed to load package...
```

### 2. Start Docker Cluster

```bash
cp hadoop-project/hadoop.env.example hadoop-project/hadoop.env
bash scripts/start_cluster.sh
bash scripts/upload_logs.sh
```

Default setup includes:

- `master` (NameNode + ResourceManager)
- `worker1` (DataNode + NodeManager)
- `worker2` (DataNode + NodeManager)

### 3. Distributed Analysis (Spark + Hadoop)

```bash
cp spark/.env.example spark/.env
python spark/analysis.py
```

Spark pipeline outputs include:

- `1_component_volume.parquet`
- `2_severity_levels.parquet`
- `3_most_frequent_actions.parquet`
- `4_anomaly_features.parquet`

### 4. Dashboard

```bash
python -m streamlit run dashboard/app.py
```

The dashboard presents six report sections:

1. Component Landscape
2. Time-Series Behavior
3. Risk and Anomaly Assessment
4. Severity Distribution
5. Event Template Concentration
6. Cluster Efficiency Comparison

---

## Cluster Benchmark Snapshot

Measured stage times in this project:

| Stage | 1 Worker Node | 2 Worker Nodes | Speedup (1W / 2W) |
|------|---------------:|---------------:|------------------:|
| Component Volume | 147.87s | 71.59s | 2.07x |
| Severity Levels | 98.76s | 44.95s | 2.20x |
| Top Event Templates | 736.07s | 300.09s | 2.45x |
| Anomaly Features | 1079.15s | 435.62s | 2.48x |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Spark | Distributed log processing |
| Apache Hadoop / HDFS | Distributed file storage |
| Docker / Docker Compose | Reproducible multi-service cluster runtime |
| PySpark | Spark Python API |
| Streamlit | Interactive report dashboard |
| Plotly | Interactive line/bar/scatter charts |
| Altair | Donut visualization and layered annotations |
| scikit-learn | Isolation Forest anomaly detection |
| Python | Core scripting |

---

## Installation

```bash
git clone https://github.com/lav-IDE/log-analyzer.git
cd log-analyzer
pip install -r requirements.txt
```

Requirements:

- Docker + Docker Compose
- Python 3.7+

To use the full 27GB Windows dataset, download it from [logpai/loghub](https://github.com/logpai/loghub) and place it in `data/raw_logs/`.

Stop the cluster when done:

```bash
bash scripts/stop_cluster.sh
```

---



---

## Dataset Citation

If you use the Windows log dataset, please cite:

> Jieming Zhu, Shilin He, Pinjia He, Jinyang Liu, Michael R. Lyu. *Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics.* IEEE ISSRE, 2023. https://github.com/logpai/loghub

---

## Author

**Lavanya Dharmadhikari**
[LinkedIn](https://linkedin.com/in/lav465) · [GitHub](https://github.com/lav-IDE)