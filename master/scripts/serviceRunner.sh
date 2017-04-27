#!/bin/bash

CP=".:./lib/*"
echo "Running with CP:$CP $1"
MAIN=com.liquidlabs.vso.deployment.ServiceTestHarness
${JAVA_HOME}/bin/java -cp "$CP" "$MAIN" "$1"