set DIST=..\..\master\dist
set WIX=C:\Program Files\Windows Installer XML v3.5
set PATH=%WIX%\bin;%PATH%
set WORK=c:\temp\vscape-e
set ROOTDIR=logscape
set VERSION=3.5_b0615
rmdir /s /q %WORK%
mkdir %WORK%
unzip %DIST%\LogScape-%VERSION%.zip -d %WORK%
heat dir %WORK%\%ROOTDIR% -gg -cg Logscape -dr TARGETDIR -sfrag -o logscapefiles.wxs
candle -nologo -ext WixUIExtension -ext WixUtilExtension logscape.wxs logscapefiles.wxs -dPlatform=x64
light -nologo -ext WixUIExtension -ext WixUtilExtension -b %WORK%\\%ROOTDIR% logscape.wixobj logscapefiles.wixobj -o %DIST%\Logscape-%VERSION%-x64-setup.msi
del %DIST%\*.wixpdb
echo Done at %time%
