@echo off
setlocal EnableExtensions
cd /d "%~dp0"

where pwsh >nul 2>&1
if %errorlevel%==0 (
  pwsh -NoProfile -ExecutionPolicy Bypass -File ".\ImportExcel.ps1"
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -File ".\ImportExcel.ps1"
)
endlocal
pause