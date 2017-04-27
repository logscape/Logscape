SETLOCAL ENABLEEXTENSIONS

echo off

REM
REM ======= This file reads configuration information from setup.conf and applies those changes to the installation
REM

set JAVA=java

set ARG32=%~1

REM echo CONFIGURE.bat ====ARG32=(%ARG32%)
REM echo CONFIGURE.bat ====JAVA_HOME=(%JAVA_HOME%)

if NOT "%JAVA_HOME%" == "" (
	REM echo CONFIGURE.bat ************ ===============  using JAVA_HOME  =========== ************
	set JAVA="%JAVA_HOME%\bin\java"
)

if NOT "%ARG32%" =="" (
	REM echo CONFIGURE.bat ************ =============== Using JAVA Arg =========== ************
	set JAVA="%ARG32%\bin\java"
)

REM echo CONFIGURE.bat ************ =============== Running ApplyConfig with JAVA=%JAVA%
%JAVA% -Xms32m -Xmx32m -cp "%~dp0\..\boot.jar" com.liquidlabs.ffilter.FileFilter "%~dp0\.."

REM echo CONFIGURE.bat *********** COPY logscape.ini to 64bit version  %~dp0\logscape.exe
copy "%~dp0\..\logscape.ini" "%~dp0\..\logscape64.ini"

REM echo CONFIGURE.bat *********** DONE EXE Replace

