#!/bin/bash
#
# logscape      This shell script takes care of starting and stopping logscape
#
# chkconfig: 345 80 20
#
### BEGIN INIT INFO
# Provides: logscape
# Required-Start: $network $syslog
# Required-Stop: $network $syslog
# Default-Start:
# Default-Stop:
# Description: Logscape v1.0
# Short-Description: start and stop logscape
### END INIT INFO

RETVAL=0

################ README ############
####
#### Set MANAGEMENT_HOST host

################ JAVA_HOME ############
### If Java6+ is not on the path - then setup the following
### export JAVA_HOME=/var/lib/jre
### export PATH=$JAVA_HOME/bin:$PATH

###########
##  how to make logscape start at boot time [systemv service]
##   1. cp logscape.sh logscape
##   2. Edit logscape
##      - set up the value for LOGSCAPE_USER to be the user you want to start logscape when started as a service
##      - change SYSTEM_SERVICE value to 1
##   3. su root
##   4. mv logscape /etc/init.d 
##   2. Install script using - 'chkconfig --add logscape' 
##
##########

MANAGEMENT_HOST=localhost
MANAGEMENT_PORT=11000
MANAGER=stcp://$MANAGEMENT_HOST:$MANAGEMENT_PORT
AGENT_COUNT=1
LOGSCAPE_HOME=.

## NEED TO BE SETUP UP IF YOU WANT TO START LOGSCAPE AS A SERVICE (default: user called 'logscape')
LOGSCAPE_USER=logscape
## if set to 1, behave as boot-service, if 0, behave as script
SYSTEM_SERVICE=0

## change this to be an absolute path when running via init-d/sysctl
ROOT=.

echo "===========  ARG:" $1

cd $LOGSCAPE_HOME

start() {


	if [ "$SYSTEM_SERVICE" = "1" ]; then
		if [ -e "agent.lock" ]; then
			echo "\tDeleting stale [agent.lock] ... "
			rm agent.lock --verbose
		fi
	fi

	if [ -e "agent.lock" ]; then
		RETVAL=-1
		echo "[WARN] Agent.lock is present. Is an instance of Logscape already running?"
		echo "Press Ctrl-C to prevent restart operation (sleep 10s)..."
		sleep 5
		echo "..."
		sleep 5
		echo "[WARN] Stopping existing processes..."
		stop
		sleep 30
	fi

	echo "STARTING Logscape boot:"  $BOOT;
	echo "The browser will start on port:8080 with user admin/admin"

    mv boot.log boot.log.1 > /dev/null 2>&1

	if [ "$SYSTEM_SERVICE" = "1" ]; then
 		/bin/su $LOGSCAPE_USER -c "nohup ./boot.sh  $MANAGER  > boot.log 2>&1 &"
   	else
		nohup ./boot.sh  $MANAGER > boot.log 2>&1 &
	fi
	

	RETVAL=$?
	sleep 4
	cat boot.log
	echo Showing status.txt
	cat status.txt
}
function stop(){
    echo "STOPPING LogScape"
    for i in $( ls  ./pids/agent-*.pid ); do
	    echo "Stopping `cat $i`"
		kill -TERM  `cat $i`;
    done
# optionally use the kill below - all LS processes execute with -DLOGSCAPE so can be filtered as follows
#kill `ps -ef | grep logscape | awk '{printf("%s\n",$2);}' `
	RETVAL=$?
	rm ./pids/agent-*.pid
}

function status(){
    cat status.txt
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
