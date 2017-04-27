### Set to true if you want to run in headless mode - i.e. no console output
set HEADLESS=true
set INSTALL_COUNT=6
set DESKTOP_COUNT=1
set BUILD=\workspace2\master
set INSTALL=\vscape-install
set HOST=localhost

cd %BUILD%

echo "**************** BUILDING VSPAPE from %BUILD%!! ***************************** "


REM ant clean dist

echo "Cleaning install location"
rmdir  /S /Q %INSTALL%
mkdir %INSTALL%
echo "****************** INSTALLING....VSCAPE initial INTO $INSTALL/VScape *********************"

xcopy /S /I %BUILD%\dist\vscape.zip %INSTALL%


cd %INSTALL%
unzip vscape.zip
cd vscape

echo "****************** STARTING VSCAPE *********************"

REM ############ LOOKUP
xcopy /Y /S /Q /I %INSTALL%\vscape %INSTALL%\vscapeLU
cd %INSTALL%\vscapeLU

REM ############ LOOKUP/FAILOVER


SET COUNT=0
:Loop

echo "****************** STARTING VSCAPE %COUNT% OF %INSTALL_COUNT% *********************"

xcopy /Y /S /Q /I %INSTALL%\vscape %INSTALL%\vscape%COUNT%
cd %INSTALL%\vscape%COUNT%

If %COUNT% == %INSTALL_COUNT% goto Exit

sleep 1
SET /A COUNT=COUNT+1
ECHO %COUNT%
goto Loop

:Exit
echo "****************** DONE *********************"

cd %BUILD%