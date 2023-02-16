@echo off

set java_cmd=java
set javac_cmd=javac

set jvm_cp=
set jvm_ld_path=
set jvm_opts=
set java_command=
set javac_command=

if defined JAVA_HOME (
	set java_cmd="%JAVA_HOME%\bin\java"
	set javac_cmd="%JAVA_HOME%\bin\javac"
)

set jvm_cp=-cp "%FMPJ_HOME%\lib\mpj.jar;%FMPJ_HOME%\lib\runtime.jar;%FMPJ_HOME%\lib\jhwloc.jar"
set jvm_ld_path=-Djava.library.path="%FMPJ_HOME%\lib"
for /f "tokens=2 delims==" %%b in ('findstr /b fmpjDaemonJvmOpts "%FMPJ_HOME%\conf\runtime_win.properties"') do @set jvm_opts=%%b
set java_command=%java_cmd% %jvm_opts% %jvm_cp% %jvm_ld_path%
set javac_command=%javac_cmd% -cp "%FMPJ_HOME%\lib\mpj.jar"

exit /b 0
