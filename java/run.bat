@echo off
setlocal enabledelayedexpansion

REM One Billion Row Challenge - Java Interactive Runner Script (Windows)

:MENU
cls
echo.
echo ========================================================================
echo   One Billion Row Challenge - Java Benchmark Runner
echo ========================================================================
echo.
echo Please select a benchmark to run:
echo.
echo   [1] Quick Test        (1K rows    - instant)
echo   [2] Small Benchmark   (10M rows   - ~30 seconds)
echo   [3] Large Benchmark   (100M rows  - ~20 minutes)
echo   [4] Billion Challenge (1B rows    - ~45 minutes!)
echo.
echo   [0] Exit
echo.
set /p CHOICE="Enter your choice (0-4): "

if "%CHOICE%"=="0" goto END
if "%CHOICE%"=="1" set SIZE=1k& set ROWS=1000& set FILE=measurements-1k.txt& goto SIMD_CHOICE
if "%CHOICE%"=="2" set SIZE=10m& set ROWS=10000000& set FILE=measurements-10m.txt& goto SIMD_CHOICE
if "%CHOICE%"=="3" set SIZE=100m& set ROWS=100000000& set FILE=measurements-100m.txt& goto SIMD_CHOICE
if "%CHOICE%"=="4" set SIZE=1b& set ROWS=1000000000& set FILE=measurements-1b.txt& goto SIMD_CHOICE

echo.
echo Invalid choice! Please select 0-4.
timeout /t 2 >nul
goto MENU

:SIMD_CHOICE
set DATA_PATH=..\data\%FILE%
cls
echo.
echo ========================================================================
echo   SIMD Optimization
echo ========================================================================
echo.
echo Do you want to enable SIMD (Single Instruction, Multiple Data)?
echo.
echo   SIMD can provide performance improvements on supported hardware.
echo.
echo   [1] Yes - Enable SIMD
echo   [2] No  - Standard mode
echo.
set /p SIMD_CHOICE="Enter your choice (1-2): "

set SIMD_FLAG=
if "%SIMD_CHOICE%"=="1" set SIMD_FLAG=--simd

if "%SIMD_CHOICE%"=="1" (
    set SIMD_TEXT=with SIMD enabled
) else (
    set SIMD_TEXT=in standard mode
)

:CHECK_BUILD
REM Check if classes directory exists
if not exist "target\classes" (
    echo.
    echo ========================================================================
    echo Building project first...
    echo ========================================================================
    echo.

    REM Create target directory structure
    if not exist "target\classes" mkdir target\classes

    REM Compile Java sources with javac
    echo Compiling Java sources...
    javac --add-modules jdk.incubator.vector ^
          -d target\classes ^
          -sourcepath src\main\java ^
          src\main\java\com\onebillion\*.java ^
          src\main\java\com\onebillion\strategies\*.java ^
          src\main\java\com\onebillion\result\*.java

    if errorlevel 1 (
        echo.
        echo Build failed!
        pause
        goto MENU
    )
    echo.
    echo Build successful!
    echo.
)



:RUN_BENCHMARK
cls
echo.
echo ========================================================================
echo   Running Benchmark - %SIZE% rows %SIMD_TEXT%
echo ========================================================================
echo.
echo Data file: %DATA_PATH%
echo.

REM Run the application
java --add-modules jdk.incubator.vector -cp target\classes com.onebillion.Main %SIMD_FLAG% %DATA_PATH%

echo.
echo ========================================================================
echo   Benchmark Complete!
echo ========================================================================
echo.
echo.
set /p AGAIN="Run another benchmark? (Y/N): "
if /i "%AGAIN%"=="Y" goto MENU

:END
echo.
echo Thank you for using the One Billion Row Challenge runner!
echo.
endlocal
