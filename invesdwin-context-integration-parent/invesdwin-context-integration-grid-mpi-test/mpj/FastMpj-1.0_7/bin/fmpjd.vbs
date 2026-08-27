
Set WshShell = WScript.CreateObject("WScript.Shell")
Set UserEnv = WshShell.Environment("User")
Set ProccessEnv = WshShell.Environment("Process")
Set SystemEnv = WshShell.Environment("System")

fmpj_home = UserEnv("FMPJ_HOME")

If fmpj_home = "" Then
	fmpj_home = ProccessEnv("FMPJ_HOME")

	If fmpj_home = "" Then
		fmpj_home = SystemEnv("FMPJ_HOME")
	
		If fmpj_home = "" Then
			Wscript.Echo "FMPJ_HOME must be set"
			Wscript.Quit
		End if
	End if
End if

java_home = UserEnv("JAVA_HOME")

If java_home = "" Then
	java_home = ProccessEnv("JAVA_HOME")

	If java_home = "" Then
		java_home = SystemEnv("JAVA_HOME")
	End if
End if

java_classpath = "-cp " & chr(34) & fmpj_home & "\lib\runtime.jar;" & fmpj_home & "\lib\mpj.jar;" & fmpj_home & "\lib\jhwloc.jar" & chr(34)
java_ld_path = "-Djava.library.path=" & chr(34) & fmpj_home & "\lib\" & chr(34)
Set namedArguments = WScript.Arguments.Named

If namedArguments.Exists("Command") Then
	java_command = namedArguments.Item("Command")
Else
	If java_home = "" Then
		java_command = "java"
	Else
		java_command = chr(34) & java_home & "\bin\java" & chr(34)
	End if
End if

If namedArguments.Exists("Class") Then
	java_class = namedArguments.Item("Class")
Else
	java_class = "runtime.FMPJRunDaemon"
End if

If namedArguments.Exists("Hostname") Then
	machine = namedArguments.Item("Hostname")
Else
	'This means local machine
	machine = "."
End if

command = java_command & " " & java_ld_path & " " & java_classpath & " " & java_class
Set objWMIService = GetObject("winmgmts:" & "{impersonationLevel=impersonate}!\\" & machine & "\root\cimv2")
Set objProcess = objWMIService.Get("Win32_Process")
Set objStartup = objWMIService.Get("Win32_ProcessStartup")
Set objConfig = objStartup.SpawnInstance_()
objConfig.ShowWindow = 0

errReturn = objProcess.Create(command, NULL, objConfig, intProcessID)

'If errReturn <> 0 Then
	WScript.Echo "Error " & errReturn
	Wscript.Echo "fmpj_home = " & fmpj_home
	Wscript.Echo "java_home = " & java_home
	WScript.Echo "java_command = " & java_command
	WScript.Echo "java_ld_path = " & java_ld_path
	WScript.Echo "java_classpath = " & java_classpath
	WScript.Echo "java_class = " & java_class
	WScript.Echo "machine = " & machine
	WScript.Echo "command = " & command
'End if

