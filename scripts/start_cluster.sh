#!/bin/bash

echo "Starting Hadoop cluster..."

# Start HDFS
start-dfs.sh

# Start YARN
start-yarn.sh

echo ""
echo "Cluster status:"
jps

echo ""
echo "HDFS UI: http://localhost:9870"
echo "YARN UI: http://localhost:8088"
