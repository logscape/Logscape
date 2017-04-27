SETLOCAL ENABLEEXTENSIONS


REM
REM ======= This dumps a summary of the files and their sizes being collected by LogScape
REM

echo off

set JAVA=java

set ARG32=%~1

echo  ====ARG32=(%ARG32%) 
echo  ====JAVA_HOME=(%JAVA_HOME%)
REM set LS=..\build\logscape
set LS=..
set LS_LIB=%LS%\system-bundles
set CP=%LS_LIB%\lib-1.0\thirdparty\thirdparty-all.jar;%LS_LIB%\boot\lib\*;%LS_LIB%\vs-log-1.0\lib\vs-log.jar
echo "CP is" %CP%
if NOT "%JAVA_HOME%" == "" (
	set JAVA="%JAVA_HOME%\bin\java"
)

if NOT "%ARG32%" =="" (
	set JAVA="%ARG32%\bin\java"
)

%JAVA% -cp %CP% com.liquidlabs.log.DBDump %LS%

