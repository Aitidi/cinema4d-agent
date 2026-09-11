@echo off
chcp 65001 >nul
echo 正在启动 Cinema 4D Agent，请稍候...
set "SCRIPT_DIR=%~dp0"
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%bin\start-cinema4d-agent.ps1" -Action Start
set "EXIT_CODE=%ERRORLEVEL%"
echo.
if not "%EXIT_CODE%"=="0" (
  echo 启动未完全成功，请查看上面的提示。
) else (
  echo Cinema 4D Agent 已准备就绪。
)
pause
exit /b %EXIT_CODE%
