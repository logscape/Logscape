' Process.vbs
' VBScript Win32_Process to discover which processes are running
' Author Guy Thomas http://computerperformance.co.uk/
' Version 1.4 - December 2010
' -------------------------------------------------------'
Option Explicit
Dim objWMIService, objProcess, colProcess, qList
Dim strComputer, strList,qItem,WshNetWork

strComputer = "."
Set WshNetwork = WScript.CreateObject("WScript.Network")

Set objWMIService = GetObject("winmgmts:" _
& "{impersonationLevel=impersonate}!\\" _
& strComputer & "\root\cimv2")

Set qList = objWMIService.ExecQuery (" SELECT Name, IDProcess, PrivateBytes, PercentProcessorTime FROM Win32_PerfFormattedData_PerfProc_Process WHERE Name <> '_Total' AND PercentProcessorTime <> 0 AND Name <> 'wmiprvse' AND Name <> 'Idle'")
For Each qItem in qList
		WSCript.Echo Now() & "," & WshNetwork.ComputerName & "," & qItem.Name & "," & qItem.IDProcess & "," & qItem.PercentProcessorTime & "," & qItem.PrivateBytes
Next


WScript.Quit

' End of List Process Example VBScript
