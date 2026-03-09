#!/bin/bash

echo "Stopping Hadoop cluster..."

# Stop YARN
stop-yarn.sh

# Stop HDFS
stop-dfs.sh

echo ""
echo "Cluster status:"
jps
