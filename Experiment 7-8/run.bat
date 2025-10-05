@echo off
title Distributed EHR System Launcher

REM --- Instructions ---
echo =========================================================
echo ==        Distributed EHR System Launcher              ==
echo =========================================================
echo.
echo This script will start all the necessary components for the
echo Distributed Electronic Health Record system.
echo.
echo It will open 7 new command prompt windows for the backend:
echo   - 3 Data Nodes (Ports 7001, 7002, 7003)
echo   - 3 Application Nodes (Ports 6001, 6002, 6003)
echo   - 1 API Gateway (Port 5000)
echo.
echo It will also open the Patient, Doctor, and Admin portals
echo in your default web browser.
echo.
echo To stop the system, you must close all 7 of the newly
echo opened command prompt windows.
echo.
echo =========================================================
pause
echo.

REM --- Cleanup: Delete old database files for a fresh start ---
echo Cleaning up old database files...
if exist data_node_1.db del data_node_1.db
if exist data_node_2.db del data_node_2.db
if exist data_node_3.db del data_node_3.db
echo Cleanup complete.
echo.

REM --- Start Backend Servers ---
echo Starting backend servers... Please wait.
echo.

REM Start the 3 Data Nodes in separate windows
REM The 'cmd /k' command keeps the new window open to show server logs.
start "Data Node 1 (Port 7001)" cmd /k python data_node.py 7001 data_node_1.db
start "Data Node 2 (Port 7002)" cmd /k python data_node.py 7002 data_node_2.db
start "Data Node 3 (Port 7003)" cmd /k python data_node.py 7003 data_node_3.db

REM Start the 3 Application Nodes in separate windows
start "App Node 1 (Port 6001)" cmd /k python app_node.py 6001
start "App Node 2 (Port 6002)" cmd /k python app_node.py 6002
start "App Node 3 (Port 6003)" cmd /k python app_node.py 6003

REM Give the nodes a moment to initialize before starting the gateway
timeout /t 3 /nobreak >nul

REM Start the API Gateway
start "API Gateway (Port 5000)" cmd /k python api_gateway.py

echo Backend servers are starting up in new windows...
echo.

REM --- Launch Frontend ---
echo Giving servers 5 seconds to initialize before launching frontends...
timeout /t 5 /nobreak >nul

echo Launching web portals...
start client.html
start doctor.html
start index.html

echo.
echo =========================================================
echo System startup complete!
echo =========================================================
echo.
pause
