#!/bin/bash

VERSION=3.41_b0317

echo "Going to publish Logscape:$VERSION"
echo "Remember to change the license server in /Volumes/DEV/LicenseServerTrial/trial.properties to have correct verion"
# Restart license server
#Configure the service editing
#file    /Library/LaunchDaemons/


echo "Copying dist files - expecting MSI and ZIP files"
rm dist/*
mkdir dist
cp -v ../dist/* dist
echo "Uploading .........."

. ./upload.sh Release_$VERSION
