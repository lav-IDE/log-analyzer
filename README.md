# Log Analyzer

A distributed log analysis system built with **Apache Spark** and **Apache Hadoop** to process large-scale Windows system logs — detecting errors, identifying anomalies, and visualizing system health through an interactive dashboard.

---

## What This Does

System logs contain critical signals about software health — but at scale, manually reviewing them is impossible. This project builds a distributed pipeline that:

- Ingests and processes real **Windows CBS (Component Based Servicing)** logs using **HDFS + Hadoop**
- Runs distributed analysis using **Apache Spark** for high-throughput processing
- Detects anomalous log patterns and error clusters
- Visualizes results through a **Streamlit dashboard**

---

## Dataset

This project uses the **Windows log dataset** from [logpai/loghub](https://github.com/logpai/loghub) — a benchmark log collection used by organizations including IBM, Microsoft, Huawei, and Nvidia for log analytics research.

- **Source:** Real Windows 7 CBS (Component Based Servicing) logs from `C:\Windows\Logs\CBS`
- **Full dataset size:** 27+ GB, spanning 226+ days of system activity
- **Development sample:** `Windows_2k.log` — 2,000 structured log entries used for pipeline development and testing
- **Log format:** `Date Time Level Component Content`

The `Windows_2k` sample is the standard subset from loghub used to develop and validate pipelines before scaling to the full dataset.

---

## Project Structure

```
log_analyzer/
├── log_generator/        # Synthetic log generator (for pipeline testing)
│   └── log_generator.py
├── spark/                # Spark analysis pipeline
│   └── analysis.py
├── dashboard/            # Streamlit visualization dashboard
│   └── app.py
├── data/
│   └── raw_logs/         # Windows_2k.log and generated logs
├── hadoop-project/       # Hadoop configuration
├── scripts/              # Utility scripts
├── requirements.txt
└── README.md
```

---

## Pipeline Overview

### 1. Log Ingestion
Windows CBS logs are loaded into HDFS for distributed processing. The `Windows_2k.log` sample is used during development.

Log format:
```
Date       Time     Level   Component   Content
2016-09-28 04:30:30 Info    CBS         Starting TrustedInstaller initialization.
2016-09-28 04:30:30 Warning CBS         Failed to load package...
```

### 2. Distributed Analysis (Spark + Hadoop)
```bash
cd spark/
python analysis.py
```

The Spark pipeline:
- Reads logs from HDFS
- Parses timestamps, log levels, and component fields
- Aggregates error/warning counts over time
- Flags anomalous event bursts and irregular patterns

### 3. Dashboard
```bash
cd dashboard/
python app.py
```
Interactive Streamlit dashboard for exploring parsed log results.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Spark | Distributed log processing |
| Apache Hadoop / HDFS | Distributed file storage |
| PySpark | Spark Python API |
| Streamlit | Interactive dashboard |
| Python | Core scripting |

---

## Installation

```bash
git clone https://github.com/lav-IDE/log-analyzer.git
cd log-analyzer
pip install -r requirements.txt
```

Requirements: Python 3.7+, Apache Spark 2.4+, Apache Hadoop (config in `hadoop-project/`)

To use the full 27GB Windows dataset, download it from [logpai/loghub](https://github.com/logpai/loghub) and place it in `data/raw_logs/`.

---

## Roadmap — v2

- [ ] **ML-based anomaly detection** using Isolation Forest on log frequency and error-burst features
- [ ] **Log parsing** with Drain or similar template-extraction algorithms (standard in loghub benchmarks)
- [ ] **Full 27GB dataset** processing on a multi-node Hadoop cluster

---

## Dataset Citation

If you use the Windows log dataset, please cite:

> Jieming Zhu, Shilin He, Pinjia He, Jinyang Liu, Michael R. Lyu. *Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics.* IEEE ISSRE, 2023. https://github.com/logpai/loghub

---

## Author

**Lavanya Dharmadhikari**
[LinkedIn](https://linkedin.com/in/lav465) · [GitHub](https://github.com/lav-IDE)