#!/bin/bash
rm -rf vscape*

unzip ../vscape.zip
cd vscape
chmod 755 install.sh
./install.sh
cd ..
cp -r vscape vscape1
cp -r vscape vscape2
cp -r vscape vscape3
cp -r vscape vscape4
cp -r vscape vscape5
