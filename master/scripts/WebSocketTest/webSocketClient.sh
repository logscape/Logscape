#!/bin/bash


export SCRIPT=webSocketClient.groovy
CP=lib/*

echo  ">>>>>>>>>" "$CP"

if [ -z "${JAVA_HOME}" ]; then
        java  -cp "$CP" groovy.lang.GroovyShell $SCRIPT		
else 

        $JAVA_HOME/bin/java -cp "$CP" groovy.lang.GroovyShell $SCRIPT		

fi


