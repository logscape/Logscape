#WixInstaller

##Description
WixInstaller is responsible for the building of the Logscape Windows MSI installation files.

##Configuration
The version number contained within `./logscape/MakeMsi-e.bat` and `./logscape/MakeMsi-e64.bat` should match the build you are attempting to convert to MSI format

##Dependencies
A Logscape-X.zip must be present in `dist` with a matching version number.

##Outputs
Execution of `./logscape/MakeMsi-all.bat` will result in the generation of x86 and x64 Msi files in the `dist` folder

##Usage
1. Modify ./logscape/MakeMsi-e.bat to the new build ID
2. Modify ./logscape/MakeMsi-e64.bat to the new build ID
3. Run ./logscape/MakeMsi-all.bat on the Windows environment

##Other comments