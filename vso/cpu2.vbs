Option Explicit
On Error Goto 0
Dim strSrv, strQuery
strSrv = "."

WScript.Echo GetFormattedCPU(StrSrv)
WScript.Echo "______"
WScript.Echo GetRAWCPU(StrSrv)

Function GetFormattedCPU(strSrv)
    Dim objWMIService, Item, Proc
   
    strQuery = "select * from Win32_PerfFormattedData_PerfOS_Processor"
    Set objWMIService = GetObject("winmgmts:\\" & StrSrv & "\root\cimv2")
    Set Item = objWMIService.ExecQuery(strQuery,,48)
    WScript.Echo strQuery
    For Each Proc In Item
       GetFormattedCPU = GetFormattedCPU & Proc.PercentProcessorTime & ", "
       wscript.echo "Processor " & Proc.Name & " = " & Proc.PercentProcessorTime
    Next
 
End Function

Function GetRAWCPU(StrSrv)
      Dim objWMIService, Item, Proc
    
      strQuery = "select * from Win32_PerfRawData_PerfOS_Processor"
   
      Set objWMIService = GetObject("winmgmts:\\" & StrSrv & "\root\cimv2")
      Set Item = objWMIService.ExecQuery(strQuery,,48)
     WScript.Echo strQuery
     For Each Proc In Item
         GetRAWCPU= GetRAWCPU & Proc.PercentProcessorTime & ","
         wscript.echo "Processor " & Proc.Name & " = " & Proc.PercentProcessorTime
    Next
 
End Function