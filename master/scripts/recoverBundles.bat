SETLOCAL ENABLEEXTENSIONS
echo off

set JAVA=java

echo "With no arguments, this script will use the Manager set in the Setup.conf to download the bundles (.zips)"
echo "If you give this script the host name only, it will use a different port"
echo "If you give this script hostname and port (as distinct arguments) it will use those parameters"
echo "This script will download system bundles to overwrite the agents ./downloads system bundles"
:MAIN:

if NOT "%JAVA_HOME%" == "" (
        set JAVA="%JAVA_HOME%\bin\java"
)

%JAVA% -Xms32m -Xmx32m -cp "%~dp0;%~dp0\..\lib\groovy-all-1.8.7.jar" groovy.lang.GroovyShell recoverBundles.groovy %1 %2 %3
