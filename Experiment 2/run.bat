@echo off
echo "Installing Requirements" 
pip install -r requirements.txt
cls
echo "Running the program"

REM --- Launch Backend ---
echo starting servers
start "Data Node (Port 7000)" cmd /k python data_node.py 7000 data.db
start "App Node (Port 5000)" cmd /k python app_node.py 5000

REM --- Launch Frontend ---
echo Giving servers 5 seconds to initialize before launching frontends...
timeout /t 5 /nobreak >nul

echo Launching web portals...
start client.html