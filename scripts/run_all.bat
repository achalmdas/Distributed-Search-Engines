@echo off
REM Starts the broker, 3 index shards, 3 replicas and the gateway (each in its
REM own console window), then runs the client here. Run from the repo root:
REM   scripts\run_all.bat
cd /d "%~dp0.."
if not exist logs mkdir logs

start "broker 9000" python broker\broker.py
timeout /t 1 /nobreak >nul

for %%p in (8001 8002 8003) do start "shard %%p" python index_node\index_node.py %%p
for %%p in (8011 8012 8013) do start "replica %%p" python replica\replica.py %%p
timeout /t 2 /nobreak >nul

start "gateway 8000" python gateway\gateway.py
timeout /t 2 /nobreak >nul

echo.
echo All services started. Pick 1 to search, 2 to add/update a document, 3 for sync status, 4 to quit.
echo.
python client\client.py

echo.
echo Client exited. Close the other console windows to stop the services.
