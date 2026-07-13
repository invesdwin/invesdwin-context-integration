#!/usr/bin/env bash

# The environment variable CONTAINER_ID is provided by YARN
# Example: container_1720000000000_0001_01_000001
# The last digit (or the second to last field) can be used to distinguish nodes.
# Let's say we assume the rank is determined by the container sequence number.

# Extract YARN sequence number (Remember: Subtract 2 to account for the AppMaster!)
SEQ=$(echo $CONTAINER_ID | cut -d'_' -f5)

# Convert string sequence to a standard base-10 integer in a POSIX-compliant way (works on sh, dash, and bash)
SEQ_NUM=$(expr "$SEQ" + 0)
RANK=$((SEQ_NUM - 2))
SIZE={SIZE}

# 1. Create a unique local logging folder for this container
LOCAL_LOG_DIR="/tmp/container_${CONTAINER_ID}"
mkdir -p "$LOCAL_LOG_DIR"

# 2. Execute the payload, passing the local log directory
/opt/java/openjdk/bin/java -jar $1 --size $SIZE --rank $RANK --logDir "$LOCAL_LOG_DIR"

# 3. Copy the generated log files back to HDFS so your client can download them
/home/hduser/hadoop/bin/hadoop fs -mkdir -p /tmp/logs/
/home/hduser/hadoop/bin/hadoop fs -put "$LOCAL_LOG_DIR"/* /tmp/logs/