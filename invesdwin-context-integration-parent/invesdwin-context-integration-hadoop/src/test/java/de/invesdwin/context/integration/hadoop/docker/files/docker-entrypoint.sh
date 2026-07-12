#!/bin/bash

sudo service ssh start

die_func() {
        echo "shutdown"
        exit 1
}
trap die_func TERM
trap die_func INT

echo "127.0.0.1 $(hostname)" | sudo tee -a /etc/hosts
export YARN_NODEMANAGER_HOSTNAME=localhost
export YARN_RESOURCEMANAGER_HOSTNAME=localhost

if [ ! -d "/tmp/hadoop-hduser/dfs/name" ]; then
        $HADOOP_HOME/bin/hdfs namenode -format
fi

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

$HADOOP_HOME/bin/hdfs dfs -mkdir /tmp
$HADOOP_HOME/bin/hdfs dfs -mkdir /users
$HADOOP_HOME/bin/hdfs dfs -mkdir /jars
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /users
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /jars

while pidof java
do
    sleep 10 &
    wait
done
