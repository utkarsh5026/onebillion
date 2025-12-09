@echo off
setlocal enabledelayedexpansion

:: Color codes for Windows (using ANSI escape sequences)
:: Requires Windows 10+ with VT100 support
for /F %%a in ('echo prompt $E ^| cmd') do set "ESC=%%a"

set "CYAN=%ESC%[1;36m"
set "GREEN=%ESC%[1;32m"
set "YELLOW=%ESC%[1;33m"
set "BLUE=%ESC%[1;34m"
set "MAGENTA=%ESC%[1;35m"
set "RED=%ESC%[1;31m"
set "BOLD=%ESC%[1m"
set "RESET=%ESC%[0m"

:menu
cls
echo.
echo %CYAN%╔════════════════════════════════════════════════════════════════════════╗%RESET%
echo %CYAN%║%RESET%  %BOLD%One Billion Row Challenge - Data Generator%RESET%%CYAN%                            ║%RESET%
echo %CYAN%╚════════════════════════════════════════════════════════════════════════╝%RESET%
echo.
echo %BOLD%Choose a dataset to generate:%RESET%
echo.
echo   %GREEN%[1]%RESET% Generate Small Dataset   - 10M rows %YELLOW%(~140MB, for testing)%RESET%
echo   %GREEN%[2]%RESET% Generate Large Dataset   - 100M rows %YELLOW%(~1.4GB, standard 1BRC)%RESET%
echo   %GREEN%[3]%RESET% Generate Billion Dataset - 1B rows %YELLOW%(~14GB, full challenge!)%RESET%
echo   %GREEN%[4]%RESET% Generate All Datasets    - 10M + 100M + 1B %YELLOW%(~15.5GB total)%RESET%
echo   %RED%[5]%RESET% Exit
echo.
set /p choice=%BOLD%Enter your choice [1-5]: %RESET%

if "%choice%"=="1" goto small
if "%choice%"=="2" goto large
if "%choice%"=="3" goto billion
if "%choice%"=="4" goto all
if "%choice%"=="5" goto end

echo %RED%Invalid choice! Please enter a number between 1 and 5.%RESET%
timeout /t 2 >nul
goto menu

:small
echo.
echo %BLUE%▶ Generating 10M rows%RESET% → %CYAN%measurements-10m.txt%RESET%
python generate.py 10000000
if errorlevel 1 (
    echo %RED%✗ Error during generation!%RESET%
    pause
    goto menu
)
echo %GREEN%✓ Generation complete!%RESET%
echo.
pause
goto menu

:large
echo.
echo %BLUE%▶ Generating 100M rows%RESET% → %CYAN%measurements-100m.txt%RESET%
python generate.py
if errorlevel 1 (
    echo %RED%✗ Error during generation!%RESET%
    pause
    goto menu
)
echo %GREEN%✓ Generation complete!%RESET%
echo.
pause
goto menu

:billion
echo.
echo %MAGENTA%▶ Generating 1 BILLION rows%RESET% → %CYAN%measurements-1b.txt%RESET% %YELLOW%(this will take a while!)%RESET%
python generate.py b
if errorlevel 1 (
    echo %RED%✗ Error during generation!%RESET%
    pause
    goto menu
)
echo %GREEN%✓ Generation complete!%RESET%
echo.
pause
goto menu

:all
echo.
echo %MAGENTA%▶ Generating ALL datasets...%RESET%
echo.
echo %BLUE%[1/3] Generating 10M rows...%RESET%
python generate.py 10000000
if errorlevel 1 (
    echo %RED%✗ Error during small dataset generation!%RESET%
    pause
    goto menu
)
echo %GREEN%✓ Small dataset complete!%RESET%
echo.
echo %BLUE%[2/3] Generating 100M rows...%RESET%
python generate.py
if errorlevel 1 (
    echo %RED%✗ Error during large dataset generation!%RESET%
    pause
    goto menu
)
echo %GREEN%✓ Large dataset complete!%RESET%
echo.
echo %BLUE%[3/3] Generating 1B rows...%RESET%
python generate.py b
if errorlevel 1 (
    echo %RED%✗ Error during billion dataset generation!%RESET%
    pause
    goto menu
)
echo %GREEN%✓ Billion dataset complete!%RESET%
echo.
echo %MAGENTA%✓ All datasets generated successfully!%RESET%
echo.
pause
goto menu

:end
echo.
echo %GREEN%Thank you for using the Data Generator!%RESET%
echo.
timeout /t 1 >nul
exit /b 0
