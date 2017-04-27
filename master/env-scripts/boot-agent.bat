echo off
setlocal ENABLEDELAYEDEXPANSION

set CP=.;boot.jar

%JAVA_HOME%\bin\java -Xverify:none -Dlog4j.configuration=./agent-log4j.properties -cp %CP% com.liquidlabs.boot.BootStrapper stcp://$LOOKUP_HOST:$LOOKUP_PORT $SLOTS
