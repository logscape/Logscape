Set args = Wscript.Arguments






Const BELOW_NORMAL = 16384
Const NORMAL = 32
Const ABOVENORMAL = 32768
Const HIGH = 128
Const REALTIME = 256




if args.length  < 1  Then
	WScript.Echo "usage:  renice [PID] "
	WScript.Quit
End if 


 if args.length = 1 Then

	pid=args(0)
	WScript.Echo "Setting Priority to BELOW_NORMAL"

end if 




	strComputer = "."
	Set objWMIService = GetObject("winmgmts:\\" & strComputer & "\root\cimv2")

	Set colProcesses = objWMIService.ExecQuery _
	    ("Select * from Win32_Process Where ProcessID="&pid&" ")

	WScript.Echo "Setting process below normal"

	For Each objProcess in colProcesses
	    WScript.Echo objProcess.Name
	    objProcess.SetPriority(BELOW_NORMAL)
	Next

