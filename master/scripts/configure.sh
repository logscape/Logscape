#!/bin/bash

echo "** Run from logscape/scripts: i.e. >./configure.sh **"

if [ -z "${JAVA_HOME}" ]; then
        java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell configure.groovy ../etc/ $1 $2 $3
else
        $JAVA_HOME/bin/java -Xms32m -Xmx32m -cp ../lib/*groovy* groovy.lang.GroovyShell configure.groovy ../etc/ $1 $2 $3
fi
./applyConfig.sh

echo "The Logscape Agent can now be started using 'logscape.sh start' Credentials: (admin/ll4bs)"

