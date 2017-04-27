### Set to true if you want to run in headless mode - i.e. no console output
set HEADLESS=true
set INSTALL_COUNT=5
set DESKTOP_COUNT=1
set BUILD=\workspace2\master
set INSTALL=\vscape-install
set HOST=localhost

cd %BUILD%

echo install count is %INSTALL_COUNT%
echo "Usage run-vscape.bat agents resources_per_agent sleep_after_lookup sleep_between_agents"
echo "**************** RUNNING VSCAPE from %BUILD%!! ***************************** "


REM ant clean dist

echo "Cleaning install location"
echo "****************** INSTALLING....VSCAPE initial INTO $INSTALL/VScape *********************"

cd %INSTALL%
cd vscape

echo "****************** STARTING VSCAPE *********************"

REM ############ LOOKUP
cd %INSTALL%\vscapeLU
start "VScape-LU" boot.bat -boot 11000 1
sleep 15


SET COUNT=0
:Loop

echo "****************** STARTING VSCAPE %COUNT% OF %INSTALL_COUNT% *********************"

cd %INSTALL%\vscape%COUNT%
start "VScape-%COUNT%" boot.bat stcp://%HOST%:11000 2

If %COUNT% == %INSTALL_COUNT% goto Exit

sleep 8 
SET /A COUNT=COUNT+1
ECHO %COUNT%
goto Loop

:Exit
echo "****************** DONE *********************"

cd %BUILD%
