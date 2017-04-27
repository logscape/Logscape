echo off
setlocal ENABLEDELAYEDEXPANSION


set CP=.;boot.jar

if NOT DEFINED JAVA_HOME (
echo "JAVA_HOME not set"
java -Xms32m -Xmx32m -cp %CP% com.liquidlabs.boot.BootStrapper %1 %2 %3 %4 %5
)
if DEFINED JAVA_HOME (
echo "JAVA_HOME is set"
"%JAVA_HOME%\bin\java" -Xms32m -Xmx32m -cp %CP% com.liquidlabs.boot.BootStrapper %1 %2 %3 %4 %5
)
