#!/usr/bin/env bash

#https://stackoverflow.com/questions/7334754/correct-way-to-check-java-version-from-bash-script
if type -p java; then
    echo found java executable in PATH
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    echo found java executable in JAVA_HOME
    _java="$JAVA_HOME/bin/java"
else
    echo "no java"
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" = "1.8" ]]; then
        JAVA8="true"
    else
        JAVA8="false"
    fi
fi

if [[ "$JAVA8" = "false" ]]; then
    echo "ERROR: requires Java 8 to compile against Hadoop"
    exit 1
fi

BASEDIR=`dirname $0`
cd $BASEDIR

export PWD=`pwd`

export HADOOP_HOME="${PWD}/hadoop/hadoop"
echo $HADOOP_HOME
cd MpjExpress-v0_44
ant all hadoop