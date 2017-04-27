#!/bin/bash


echo "** Run from logscape/scripts: i.e. >./networkCheck.groovy **"

if [ $# -ne 3 ]; then
  echo "Usage: ./networkCheck.sh <remoteHostAddr> <remoteHostPort> <thisHostPort>"
  exit
fi

if [ -z "${JAVA_HOME}" ]; then
        java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell networkCheck.groovy $1 $2 $3		
else
        $JAVA_HOME/bin/java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell networkCheck.groovy $1 $2 $3		

fi


