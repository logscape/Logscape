#!/bin/bash

#########################################################################
#kill  `ps -e | grep vs | awk '{print $1 }'`

if [ $# != 4 ]
then
	echo "Usaage $0 agents resources_per_agent sleep_after_lookup sleep_between_agents"
	exit 1
fi

export LC_CTYPE=UTF-8

### Set to true if you want to run in headless mode - i.e. no console output
HEADLESS=true
INSTALL_COUNT=$1
DESKTOP_COUNT=0
BUILD=~/workspace/master
INSTALL=~/vscape-install
HOST=$HOSTNAME


echo "****************** STARTING VSCAPE *********************"

############ LOOKUP
cd $INSTALL/vscapeLU
nohup ./boot.sh -boot 11000 1 > stdout.log 2>&1 &
#xterm  -geom 160 -rv -title VScapeLU -e ./boot.sh -boot 11000 1 > stdout.log 2>&1 &
sleep $3
##################################


cd $INSTALL

for ((i=1;i<=INSTALL_COUNT;i+=1)); do
    if [ $i -le $[ INSTALL_COUNT - DESKTOP_COUNT ] ]
    then
      MACHINE_PURPOSE=-Dvscape.resource.ownership=DEDICATED
    else
      MACHINE_PURPOSE=-Dvscape.resource.ownership=DESKTOP
    fi
    
	echo "********************************** Starting VScape = $i / $INSTALL_COUNT ***************************"
	cd $INSTALL/vscape$i
		nohup ./boot.sh stcp://$HOST:11000/LookupSpace $2 $MACHINE_PURPOSE > stdout.log 2>&1 &
	sleep $4
done
cd $INSTALL
find . -name *.sh -exec chmod 755 {} \;
