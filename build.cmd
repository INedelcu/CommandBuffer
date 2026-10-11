@echo off
rem Builds Debug and Release with the newest Visual Studio C++ toolset, then runs the tests.
rem   build.cmd          build both configurations and run the tests
rem   build.cmd bench    also run the benchmark (Release)
setlocal

set "VSWHERE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
for /f "usebackq tokens=*" %%i in (`"%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VSDIR=%%i"
if not defined VSDIR (
    echo Visual Studio with the C++ workload was not found.
    exit /b 1
)
rem vcvars64.bat itself looks for vswhere.exe on the PATH.
set "PATH=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer;%PATH%"
call "%VSDIR%\VC\Auxiliary\Build\vcvars64.bat" >nul || exit /b 1

cd /d "%~dp0"
for %%c in (debug release) do (
    cmake --preset %%c || exit /b 1
    cmake --build --preset %%c || exit /b 1
    ctest --preset %%c || exit /b 1
)

if /i "%~1"=="bench" build\release\CommandBufferBenchmark.exe
