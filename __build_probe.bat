@echo off
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat" >nul 2>&1
cd /d "E:\The Journey\Coding\GitHub\production\Fabric-Scheduler"
cl /nologo /std:c++20 /EHsc /W4 /MD /I include __probe.cpp /Fe:__probe.exe /link "build\Release\FabricScheduler.lib" ws2_32.lib
echo CL_EXIT=%ERRORLEVEL%
