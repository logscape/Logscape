#!/bin/bash

if [ -z "${JAVA_HOME}" ]; then
	java -cp ../lib/*groovy*:../system-bundles/lib-1.0/thirdparty/thirdparty-all.jar groovy.lang.GroovyShell  sendEmail.groovy
else
	$JAVA_HOME/bin/java -cp ../lib/*:../system-bundles/lib-1.0/thirdparty/thirdparty-all.jar groovy.lang.GroovyShell sendEmail.groovy
fi
