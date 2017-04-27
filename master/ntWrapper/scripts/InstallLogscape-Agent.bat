@echo off
setlocal


if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
set _REALPATH=%~dp0\..\

rem Decide on the wrapper binary.
set _WRAPPER_BASE=logscape
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-x86-32.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-x86-64.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%.exe
if exist "%_WRAPPER_EXE%" goto conf
echo Unable to locate a Wrapper executable using any of the following names:
echo %_REALPATH%%_WRAPPER_BASE%-x86-32.exe
echo %_REALPATH%%_WRAPPER_BASE%-x86-64.exe
echo %_REALPATH%%_WRAPPER_BASE%.exe
pause
goto :eof

rem
rem Find the wrapper.conf
rem
:conf
set _WRAPPER_CONF="conf\agent.conf"
if not %_WRAPPER_CONF%=="" goto startup
set _WRAPPER_CONF="%_REALPATH%conf\agent.conf"

rem
rem Install the Wrapper as an NT service.
rem
:startup
"%_WRAPPER_EXE%" -i %_WRAPPER_CONF%
if not errorlevel 1 goto :eof
pause

