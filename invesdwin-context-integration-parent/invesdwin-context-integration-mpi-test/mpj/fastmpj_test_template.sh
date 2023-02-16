#!/usr/bin/env bash

export FMPJ_HOME={FMPJ_HOME}
export PATH=$FMPJ_HOME/bin:$PATH
export JAVA_HOME={JAVA_HOME}

fmpjrun {ARGS}