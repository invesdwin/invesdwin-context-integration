@echo off

if not defined FMPJ_HOME (
	echo FMPJ_HOME must be set
	exit /b 127
)

call "%FMPJ_HOME%\bin\fmpj-env.bat"

if errorlevel 127 (
	echo An unexpected error has ocurred in fmpj-env.bat
	goto:eof
)

set app=runtime.FMPJRun
set command=%java_command% %app% %*
%command%
