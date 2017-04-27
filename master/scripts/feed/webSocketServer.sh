#!/bin/bash


export SCRIPT=webSocketServer.groovy
export CP="../WebSocketTest/lib/*"

echo  ">>>>>>>>>>>>" "$CP"

if [ -z "${JAVA_HOME}" ]; then
        java -Xms32m -Xmx32m -cp "$CP" groovy.lang.GroovyShell $SCRIPT		
else 

        $JAVA_HOME/bin/java -Xms32m -Xmx32m -cp "$CP" groovy.lang.GroovyShell $SCRIPT		

fi


