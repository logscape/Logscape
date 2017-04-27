SETLOCAL ENABLEEXTENSIONS
echo off

set JAVA=java

:MAIN:

if NOT "%JAVA_HOME%" == "" (
        set JAVA="%JAVA_HOME%\bin\java"
)

%JAVA% -Xms32m -Xmx32m -cp "%~dp0;%~dp0\..\lib\groovy-all-1.8.7.jar" groovy.lang.GroovyShell networkList.groovy %1 %2 %3
