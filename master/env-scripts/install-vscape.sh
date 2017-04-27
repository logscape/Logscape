#!/bin/bash

#########################################################################
#kill -9 `ps | grep vs | awk '{print $1 }'`

export LC_CTYPE=UTF-8

### Set to true if you want to run in headless mode - i.e. no console output
INSTALL_COUNT=10
BUILD=~/workspace/master
INSTALL=~/vscape-install
HOST=$HOSTNAME


cd $BUILD

echo "**************** BUILDING VSPAPE from $BUILD!! ***************************** "

ant cleanAll dist

echo "Cleaning install location"
rm -rf  $INSTALL
mkdir $INSTALL

cp $BUILD/dist/vscape.zip $INSTALL

echo "****************** INSTALLING....VSCAPE initial INTO $INSTALL/vscape *********************"

cd $INSTALL
unzip vscape.zip
cd vscape

cp -r $INSTALL/vscape $INSTALL/vscapeLU
cd $INSTALL/vscapeLU
#xterm  -geom 160 -rv -title VScapeLU -e ./boot-lookup.sh -boot stcp://$HOST:11000 &

for ((i=1;i<=INSTALL_COUNT;i+=1)); do
	echo "********************************** INSTALLING VScape = $i / $INSTALL_COUNT ***************************"
	cp -r $INSTALL/vscape $INSTALL/vscape$i
	
	if [ $i -le 3 ]; then
		cp -f $BUILD/../boot/boot.properties.mgmt $INSTALL/vscape$i/boot.properties
	fi
	
	cd $INSTALL/vscape$i
done
cd $INSTALL
find . -name *.sh -exec chmod 755 {} \;
find . -name osx_cpu -exec chmod 755 {} \;

