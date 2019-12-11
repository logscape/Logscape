Option Explicit
const fatalExitType = &H00000000
Function CheckJavaVersion()

	Dim rr
	Dim rec
	rr  = CheckJavaVersionP(Session.Property("IS_Win64Flag"),Session.Property("JAVAHOME"))
	If (Len(rr) > 0) Then
 		'MsgBox("SCRIPT FAILED***:" & rr)
 		Set rec = Session.Installer.CreateRecord(1)
 		rec.StringData(0) = rr
 		Session.Message fatalExitType, rec
 		CheckJavaVersion = 3 		
	End If
	
End Function

Function CheckJavaExists(javaHome)
	Dim fso
	Dim file
	Set fso = CreateObject("Scripting.FileSystemObject")
	file = javaHome & "\bin\java.exe"
	If (fso.FileExists(file) = false) Then
		'MsgBox("ERROR file does not exist:" + file)
		CheckJavaExists = "ERROR java.exe file does not exist, Check Java Home:" + file
	Else 
		CheckJavaExists = ""
	End If
End Function

Function CheckJavaVersionP(Win64Flag,javaHome)
	Dim objShell
	Dim line
	Dim cmd
	Dim foundJava
	Dim proc
	foundJava = CheckJavaExists(javaHome) 
	If (Len(foundJava) = 0) Then
		' only run when we are x64
		if (StrComp(Win64Flag,"yes") = 0) then
			
			' http://technet.microsoft.com/en-us/library/ee692837.aspx Running a process
			' execute java -version and see which jvm being used - Client indicates a 32 bit JVM - so show an error msg
			'Java HotSpot(TM) Client VM
			
			Set objShell = CreateObject("WScript.Shell")
			cmd = """" & javaHome & "\bin\java.exe" & """" & " -version"
			'cmd =  """C:\Program Files (x86)\Java\jdk1.6.0_29\bin\java.exe""" & " -version"
			Set proc = objShell.Exec(cmd)
			
			line = proc.StdErr.ReadAll
			if (InStr(1,line, "Client") > 0) then
				'MsgBox("ERROR: A 32 bit client JRE was specified - it cannot be used with a 64 bit MSI  " & line)
				CheckJavaVersionP = "ERROR: A 32 bit client JRE was specified - it cannot be used with a 64 bit MSI  " & line
			end if
			
		End if
	Else 
		CheckJavaVersionP = foundJava
	End If
End Function
'
' TEST needs to comment everything out
'Dim rr
'rr  = CheckJavaVersionP("no", "C:\Program Files (x86)\Java\jdk1.6.0_29")
'if (Len(rr) > 0) Then
' MsgBox("111 SCRIPT FAILED***:" & rr)
'End If
