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

# 2. Execute the payload, passing the local log directory
/opt/java/openjdk/bin/java -jar $1 --size $SIZE --rank $RANK --logDir "{HDFS_LOG_DIR}" --hdfsUri "{HDFS_URI}"
