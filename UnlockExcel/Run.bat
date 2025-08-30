@echo off
setlocal
REM Chạy PowerShell, bỏ chặn thực thi
powershell -NoProfile -ExecutionPolicy Bypass -File ".\Unlock-Xlsx-Folder.ps1"
pause