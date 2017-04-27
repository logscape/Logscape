
set CP=".;./lib/*"
echo "Running with CP:$CP $1"
set MAIN=com.liquidlabs.vso.deployment.ServiceTestHarness
echo "%JAVA_HOME%"\bin\java.exe -cp %CP% %MAIN% %*
"%JAVA_HOME%"\bin\java.exe -cp "%CP%" %MAIN% %*
