#!/bin/bash

set -e

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"

# Keep startup state and command config path consistent.
NAMENODE_DIR="/hadoop/namenode"
NAMENODE_VERSION_FILE="$NAMENODE_DIR/current/VERSION"

# Start SSH on all nodes
service ssh start

# Set JAVA_HOME in hadoop-env.sh
grep -q "^export JAVA_HOME=" "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" || \
    echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"

if [ "$NODE_TYPE" = "master" ]; then
    echo "Starting Master Node..."

    mkdir -p /hadoop/namenode /hadoop/datanode

    # Format namenode only if not already formatted
    if [ ! -f "$NAMENODE_VERSION_FILE" ]; then
        echo "Formatting NameNode..."
        hdfs --config "$HADOOP_CONF_DIR" namenode -format -force -nonInteractive
    fi

    # Start secondary namenode and resource manager as background daemons
    hdfs --config "$HADOOP_CONF_DIR" --daemon start secondarynamenode
    yarn --config "$HADOOP_CONF_DIR" --daemon start resourcemanager

    echo "Master started. HDFS UI: http://master:9870  YARN UI: http://master:8088"

    # Run namenode in foreground — keeps container alive and avoids priority error
    exec hdfs --config "$HADOOP_CONF_DIR" namenode

else
    echo "Starting Worker Node ($HOSTNAME)..."

    mkdir -p /hadoop/datanode

    # Start nodemanager as background daemon
    yarn --config "$HADOOP_CONF_DIR" --daemon start nodemanager

    echo "Worker $HOSTNAME started."

    # Run datanode in foreground — keeps container alive
    exec hdfs --config "$HADOOP_CONF_DIR" datanode
fi