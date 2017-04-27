Welcome to Logscape
===================

Installation
------------

Just Management? - You dont need to configure anything if
 - You just want to run the Manager and the WebPort 8080 is fine
 - run logscape.bat OR logscape.sh start

Otherwise, to configure Logscape from the command line:
 - stop logscape
 - cd to logscape/scripts
 - execute: configure.bat/configure.sh
 - run logscape.bat/sh or start the windows service
 - To manually install the windows service: logscape.exe -install

Startup
-------
When first installed the Management Agent will start several services; the last is the Dashboard - it may take 30 seconds 
before the web console is available.

Resource Use / Sizing
------------

 Check out the boot.properties for example configuration for servers and relative memory sizing (just below sysprops)

Reconfiguration
---------------

Note: JAVA_HOME must be configured & the Oracle JVM must be used.

Option A) is suitable for basic configuration changes - Option B) should be used when advanced properties are being added to setup.conf 

Either:
 A) - Stop LogScape 
    - Run configure.bat/configure.sh and follow the prompts (change the webport, management port etc)
    - Start LogScape
    
 B) - Stop LogScape
 	- Run configure.bat/configure.sh and follow the prompts (change the webport, management port etc)
	- Backup copies are made with a .bak extension
	- Start LogScape

Links
-----
Online Manual - http://support.logscape.com
Community Website -http://apps.logscape.com

Support
-------
support@logscape.com OR forums.logscape.com


