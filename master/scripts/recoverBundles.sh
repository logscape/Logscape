#!/bin/bash
echo "** Run from logscape/scripts: i.e. >./recoverBundles.groovy **"
echo "With no arguments, this script will use the Manager set in the Setup.conf to download the bundles (.zips)"
echo "If you give this script the host name only, it will use a different port"
echo "If you give this script hostname and port (as distinct arguments) it will use those parameters"
echo "This script will download system bundles to overwrite the agents ./downloads system bundles"

if [ -z "${JAVA_HOME}" ]; then 
	java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell  recoverBundles.groovy $1 $2 $3
else
	$JAVA_HOME/bin/java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell recoverBundles.groovy $1 $2 $3
fi
