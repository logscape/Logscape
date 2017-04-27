#!/bin/bash


echo "** Run from logscape/scripts: i.e. >./networkList.groovy **"

if [ -z "${JAVA_HOME}" ]; then
        java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell networkList.groovy $1 $2 $3		
else
        $JAVA_HOME/bin/java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell networkList.groovy $1 $2 $3		

fi


