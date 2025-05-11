@echo off
setlocal enabledelayedexpansion

:: Configuration
set "FILE=ecommerce_dataset.csv"
set "DATA_PATH=hdfs://namenode:9000/steambans.csv"
set "PYTHON_SCRIPT=app.py"
set "SPARK_MASTER=spark://spark-master:7077"

:: Clean previous results
del res*.txt 2>nul

:: Function to capture last 2 lines of output
:capture_output
set "output_file=%~1"
set "result_file=%~2"
for /f %%a in ('type "%output_file%" ^| find /c /v ""') do set /a lines=%%a
set /a lines=!lines!-2
if !lines! lss 0 set lines=0
if exist "%output_file%" (
    more +!lines! "%output_file%" >> "%result_file%"
    del "%output_file%"
)
exit /b

:: Start 1-node cluster
echo [1/8] Starting 1-node cluster...
docker-compose -f docker-compose-1.yml up -d
timeout /t 10 >nul

:: Prepare HDFS
echo [2/8] Preparing HDFS...
docker exec namenode hdfs dfsadmin -safemode leave

:: Verify and copy files
echo [3/8] Verifying and copying files...
if not exist "data\%FILE%" (
    echo Error: File data\%FILE% not found!
    exit /b 1
)
if not exist "src\%PYTHON_SCRIPT%" (
    echo Error: File src\%PYTHON_SCRIPT% not found!
    exit /b 1
)
docker cp "data\%FILE%" namenode:/
docker exec namenode hdfs dfs -put -f "%FILE%" "/%FILE%"
docker cp "src\%PYTHON_SCRIPT%" spark-master:/tmp/%PYTHON_SCRIPT%

:: Install dependencies
echo [4/8] Installing dependencies...
docker exec spark-worker-1 apk add --no-cache make automake gcc g++ python3-dev linux-headers musl-dev
docker exec spark-master apk add --no-cache make automake gcc g++ python3-dev linux-headers py3-pip musl-dev
docker exec spark-master pip3 install --no-cache-dir psutil

:: Run 1-node cluster jobs
echo [5/8] Running 1-node cluster jobs...
docker exec spark-master /spark/bin/spark-submit --master %SPARK_MASTER% "/tmp/%PYTHON_SCRIPT%" -d "%DATA_PATH%" > temp_output.txt
call :capture_output temp_output.txt res_node.txt

docker exec spark-master /spark/bin/spark-submit --master %SPARK_MASTER% "/tmp/%PYTHON_SCRIPT%" -d "%DATA_PATH%" -o > temp_output.txt
call :capture_output temp_output.txt res_node_opt.txt

:: Stop 1-node cluster
echo [6/8] Stopping 1-node cluster...
docker-compose -f docker-compose-1.yml stop

:: Start 3-node cluster
echo [7/8] Starting 3-node cluster...
docker-compose -f docker-compose-3.yml up -d
timeout /t 10 >nul

:: Prepare HDFS again
docker exec namenode hdfs dfsadmin -safemode leave
docker cp "data\%FILE%" namenode:/
docker exec namenode hdfs dfs -put -f "%FILE%" "/%FILE%"
docker cp "src\%PYTHON_SCRIPT%" spark-master:/tmp/%PYTHON_SCRIPT%

:: Run 3-node cluster jobs
echo [8/8] Running 3-node cluster jobs...
docker exec spark-master /spark/bin/spark-submit --master %SPARK_MASTER% "/tmp/%PYTHON_SCRIPT%" -d "%DATA_PATH%" > temp_output.txt
call :capture_output temp_output.txt res_nodes.txt

docker exec spark-master /spark/bin/spark-submit --master %SPARK_MASTER% "/tmp/%PYTHON_SCRIPT%" -d "%DATA_PATH%" -o > temp_output.txt
call :capture_output temp_output.txt res_nodes_opt.txt

:: Final cleanup
docker-compose -f docker-compose-3.yml stop
echo All operations completed successfully.
endlocal