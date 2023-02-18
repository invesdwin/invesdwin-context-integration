#!/bin/bash

sudo service ssh start

die_func() {
        echo "shutdown"
        exit 1
}
trap die_func TERM
trap die_func INT

if [ ! -d "/tmp/hadoop-hduser/dfs/name" ]; then
        $HADOOP_HOME/bin/hdfs namenode -format
fi

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

while pidof java
do
    sleep 10 &
    wait
done
