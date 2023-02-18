#!/usr/bin/env bash

BASEDIR=`dirname $0`
cd $BASEDIR

export PWD=`pwd`

export HADOOP_HOME="${PWD}/hadoop/hadoop"
echo $HADOOP_HOME
cd MpjExpress-v0_44
ant all hadoop