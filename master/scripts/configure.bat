SETLOCAL ENABLEEXTENSIONS
@echo off
set JAVA=java
if NOT "%1" == "" (
    if NOT "%1" == "-a" ( 
       set ARG32=%1%
    )
)



echo "** Run from the logscape\scripts directory. i.e.>configure.bat **"

if NOT "%JAVA_HOME%" == "" (
    set JAVA="%JAVA_HOME%\bin\java"
)

if NOT "%ARG32%" == "" (
    set JAVA="%ARG32%\bin\java"
    echo.
    echo "Setting java home to %JAVA%"
)

if NOT "%1%" == "-a" (
    %JAVA% -Xms32m -Xmx32m -cp "%~dp0\..\lib\groovy-all-1.8.7.jar" groovy.lang.GroovyShell configure.groovy "%~dp0/../etc/"
)

if "%1%" == "-a" (
    %JAVA% -Xms32m -Xmx32m -cp "%~dp0\..\lib\groovy-all-1.8.7.jar" groovy.lang.GroovyShell configure.groovy "%~dp0/../etc/" "%2" "%3" "%4"
)

call applyConfig.bat

echo "The Logscape Agent can now be started by starting the service or logscape.bat, Credentials: (admin/ll4bs)"
