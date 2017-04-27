SETLOCAL ENABLEEXTENSIONS


echo off

set JAVA=java

echo  ====JAVA_HOME=(%JAVA_HOME%)
set LS=%~dp0\..
set LS_LIB=%LS%\system-bundles
set CP=%LS_LIB%\lib-1.0\thirdparty\thirdparty-all.jar;%LS_LIB%\boot\lib\*;%LS_LIB%\lib-1.0\thirdparty\*
echo "CP is" %CP%
if NOT "%JAVA_HOME%" == "" (
	set JAVA="%JAVA_HOME%\bin\java"
)

%JAVA% -Xms32m -Xmx32m -cp "%CP%" com.liquidlabs.common.compression.Lz4Expand %1

