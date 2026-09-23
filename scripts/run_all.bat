@echo off

REM Move from scripts folder to project root
cd /d "%~dp0.."

echo ==========================================
echo Starting Distributed Search Engine
echo ==========================================

echo Starting Shard 1...
start "Shard 1 - 8001" cmd /k python index_node\index_node.py 8001

echo Starting Shard 2...
start "Shard 2 - 8002" cmd /k python index_node\index_node.py 8002

echo Starting Shard 3...
start "Shard 3 - 8003" cmd /k python index_node\index_node.py 8003

REM Give the shards time to start
timeout /t 2 /nobreak >nul

echo Starting Gateway...
start "Gateway - 8000" cmd /k python gateway\gateway.py

REM Give the gateway time to start
timeout /t 2 /nobreak >nul

echo Starting Client...
start "Client" cmd /k python client\client.py

echo.
echo ==========================================
echo Everything started!
echo ==========================================

pause