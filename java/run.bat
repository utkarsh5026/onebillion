@echo off
REM One Billion Row Challenge - Java Runner Script (Windows)

echo.
echo ===================================
echo  One Billion Row Challenge - Java
echo ===================================
echo.

REM Check if classes directory exists
if not exist "target\classes" (
    echo Building project first...
    call make build
    if errorlevel 1 (
        echo Build failed!
        exit /b 1
    )
)

REM Run the application
echo Running benchmark...
echo.
cd target\classes
java com.onebillion.Main %*
cd ..\..

echo.
echo Done!
