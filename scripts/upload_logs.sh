#!/bin/bash

echo "Uploading logs to HDFS..."

hdfs dfs -mkdir -p /raw_logs

hdfs dfs -put -f data/raw_logs/generated_logs/* /raw_logs/

echo "Upload complete."

hdfs dfs -ls /raw_logs
