SETLOCAL ENABLEEXTENSIONS
echo off

set JAVA=java

echo "** Run from the logscape\scripts directory. i.e.>networkCheck.bat **"


if "%1"=="" goto :USAGE:
if "%2"=="" goto :USAGE:
if "%3"=="" goto :USAGE:
goto :MAIN:

:USAGE:
echo "Usage: networkCheck.bat <remoteHostAddr> <remoteHostPort> <thisHostPort>"


:MAIN:

if NOT "%JAVA_HOME%" == "" (
	set JAVA="%JAVA_HOME%\bin\java"
)

%JAVA% -Xms32m -Xmx32m -cp "%~dp0;%~dp0\..\lib\groovy-all-1.8.7.jar" groovy.lang.GroovyShell networkCheck.groovy %1 %2 %3