#!/bin/bash
##### nicked from boot.properties tp check the Management value
sysprops=-DVSCAPE -Dvso.boot.lookup.replication.port=15000 -Dvso.lookup.peers=stcp://localhost:15000,stcp://localhost:25000 -Dvso.resource.type=XXX -Dlog4j.configuration=./agent-log4j.properties -Dweb.app.port=8080 -Dfile.encoding=ISO-8859-1 -Dvso.location=LDN 
#
# vscape      This shell script takes care of starting and stopping vscape
#
# chkconfig: 345 80 20
#
### BEGIN INIT INFO
# Provides: vscape
# Required-Start: $network $syslog
# Required-Stop: $network $syslog
# Default-Start:
# Default-Stop:
# Description: VScape v1.0
# Short-Description: start and stop vscape
### END INIT INFO

export LC_CTYPE=UTF-8
RETVAL=0

################ README ############
####
#### Make the following changes below
####
#### 1. Set JAVA_HOME if not on the PATH
#### 2. set BOOT=[agent,lookup]
#### 3. set SLOTS=1 [agent] 6 [lookup]
#### 4. set LOOKUP host

################ JAVA_HOME ############
### If Java6 is not on the path - then setup the following
### export JAVA_HOME=/var/lib/jre
### export PATH=$JAVA_HOME/bin:$PATH

###########
## setup for autoboot
##   1. Copy to /etc/init.d as vscape
##   2. Install script using - 'chkconfig --add vscape' 
##
##########

## use values - Management, Agent, agents, all
#      - lookup = the main management host where the webserver and other services run
#      - agent = run a single agent - be sure to change [LUHOST=localhost] (below) to point to the lookuphost
#      - agents = where multiple agents are to be run (make sure you copy vscape dir contents to vscape[1-n] for the agent count.
#      - all = where lookup and agents are to be executed on a single machine 

BOOT=Management
SLOTS=6
MANAGEMENT_HOST=localhost
MANAGEMENT_PORT=11000
AGENT_COUNT=1


## change this to be an absolute path when running via init-d/sysctl
ROOT=.
TMP=./output

for arg in $*
do
  echo "Arg #$index = '$arg'"
  	if [ $arg = "-boot=Management" ]; then
		BOOT=lookup
    fi
	if [ $arg = "-boot=Agent" ]; then
		BOOT=agent
	fi
	if [ $arg = "-boot=agents" ]; then
		BOOT=agents
	fi
	if [ $arg = "-boot=all" ]; then
		BOOT=all
	fi
done


echo "===========  ARG:" $1


start() {
	mkdir $TMP

	echo "STARTING VScape boot:"  $BOOT;

	echo `date` "STARTING " > $TMP/vscape-status.log

	if [ "$BOOT" = "Management" ]; then
	
	   echo "THIS is a the BOOT Agent";
	   ## boot the master agent - with 4 slots for management services
	    nohup ./boot.sh -boot $MANAGEMENT_PORT 4 > $TMP/lookup.log 2>&1 &
	else
		echo "Starting Agent"
	 	nohup ./boot.sh  stcp://$MANAGEMENT_HOST:$MANAGEMENT_PORT $SLOTS > $TMP/agent.log 2>&1 &
        sleep 2;
	fi
	
##	if [ "$BOOT" = "agents" ]; then
##		echo "THIS is regular Agent"
##	    for ((i=1;i<AGENT_COUNT+1;i+=1)); do
##			echo "Starting Agent$i-1"
##		    cd $ROOT/vscape$i;
##		 	nohup ./boot.sh  stcp://$LUHOST:$LUPORT $SLOTS > $TMP/agent-$i.log 2>&1 &
##	    done
##	        sleep 2;
##	fi
##	if [ "$BOOT" = "all" ]; then
##		echo "Booting LUSpace and Agents"
##		### LUSpace
##	    cd $ROOT/vscape;
##	    nohup ./boot.sh -boot $LUPORT 1 > $TMP/lookup.log 2>&1 &
##		### Agents
##       sleep 10;
##	    for ((i=1;i<AGENT_COUNT+1;i+=1)); do
##			echo "Starting Agent$i-1"
##		    cd $ROOT/vscape$i;
##		 	nohup ./boot.sh  stcp://$LUHOST:$LUPORT $SLOTS > $TMP/agent-$i.log 2>&1 &
##	        sleep 3;
##	    done
##	fi
	RETVAL=$?
    echo "VScape is RUNNING"
    echo `date` " RUNNING" > $TMP/vscape-status.log
}
function stop(){
    echo "STOPPING VScape"
    for i in $( ls  /tmp/agent-*.pid ); do
	    echo "Stopping `cat $i`"
		kill -TERM  `cat $i`;
    done
	RETVAL=$?
    echo "STOPPED VScape"
    echo `date` " STOPPED" > $TMP/vscape-status.log
	rm /tmp/agent-*.pid
}

function status(){
    echo VScape is:`cat $TMP/vscape-status.log`;
}
function restart(){
    stop
	echo "Stopped, Waiting for 60 seconds before starting"
    sleep 60
	echo "Starting..."
    start
}

# See how we were called.
case "$1" in
  start)
    start
	;;
  stop)
    stop
    ;;
  status)
    status
	RETVAL=$?
    ;;
  restart | condrestart)
    restart
    ;;
  *)
    echo $"Usage: $0 {start|stop|status|condrestart|restart}"
    RETVAL=1
esac

exit $RETVAL
