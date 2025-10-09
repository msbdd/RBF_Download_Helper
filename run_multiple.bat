@echo off

start "" cmd /k "path\to\app.exe --config path\to\config1.cfg"
start "" cmd /k "path\to\app.exe --config path\to\config2.cfg"
start "" cmd /k "path\to\app.exe --config path\to\config3.cfg"

echo Launched everything, have a nice day.
pause