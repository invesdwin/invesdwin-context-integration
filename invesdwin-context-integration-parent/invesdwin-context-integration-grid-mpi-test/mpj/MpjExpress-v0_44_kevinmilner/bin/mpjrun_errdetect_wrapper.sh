#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SLEEP_TIME=5

# detect the daemon port
DPORT=$(fgrep mpjexpress.mpjdaemon.port.1 $DIR/../conf/mpjexpress.conf | grep -v \# | tail -n 1)
# isolate just the value (after the = sign)
DPORT=${DPORT#*=}

# detect the daemon port
SPORT=$(fgrep mpjexpress.mpjrun.port $DIR/../conf/mpjexpress.conf | grep -v \# | tail -n 1)
# isolate just the value (after the = sign)
SPORT=${SPORT#*=}

echo "Sleeping for $SLEEP_TIME seconds"
sleep $SLEEP_TIME

MACHINEFILE=$1
shift

NP=`wc -l < $MACHINEFILE`
echo "Running on $NP processors: "`cat $MACHINEFILE`

if [[ $NP -le 0 ]]; then
  echo "invalid NP: $NP"
  exit 1
fi

MAX_TRIES=5
TRIES=0
ORIG_SLEEP_TIME=$SLEEP_TIME

echo "COMMAND: $@"

while [[ $TRIES -le $MAX_TRIES ]];do
	if [[ $TRIES -gt 0 ]];then
		echo "retry $TRIES"
	fi
	# choose a new random port to try
	DPORT=$(shuf -i 62000-64999 -n 1)
	SPORT=$(shuf -i 60000-61999 -n 1)
	echo "Daemon port: $DPORT"
#	lsof -i :$DPORT
	echo "Server port: $SPORT"
#	lsof -i :$SPORT
	echo "Booting MPJ daemons"
	java -jar $MPJ_HOME/lib/daemonmanager.jar -boot -port $DPORT -m $MACHINEFILE
	ret=$?
	success=0
	if [[ $ret -eq 0 ]];then
		success=1
	fi
	if [[ $success -eq 1 ]];then
		echo "Running MPJ"
		java -jar $MPJ_HOME/lib/starter.jar -machinesfile $MACHINEFILE -np $NP -dport $DPORT -sport $SPORT $@
		ret=$?
		echo "exit code: $ret"
	fi
	
	echo "Halting MPJ daemons"
	java -jar $MPJ_HOME/lib/daemonmanager.jar -halt -port $DPORT -m $MACHINEFILE
	
	if [[ $ret -eq 3 ]];then
		echo "detected failure code, retrying"
	else 
		echo "run succeeded or normal error, exiting. exit code: $ret"
		exit $ret
	fi
	let SLEEP_TIME=ORIG_SLEEP_TIME+SLEEP_TIME
	echo "sleeping for $SLEEP_TIME seconds"
	sleep $SLEEP_TIME
	let TRIES=TRIES+1
done
