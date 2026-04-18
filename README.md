# Real-Time Forensic Audit Pipeline (Ghost Vendor Detector)

This project detects suspicious financial behavior in real time using Kafka + Spark + graph analytics + ML risk scoring, and visualizes everything in Streamlit.

## Project Layout

```text
project-root/
|-- app_ui.py
|-- neo4j_sink.py
|-- requirements.txt
|-- config/
|   `-- kafka_config.py
|-- consumer/
|   `-- spark_stream_processor.py
|-- data/
|   `-- sample_transactions.json
|-- graph/
|   `-- graph_analysis.py
|-- output/
|   |-- flagged_cases/
|   `-- runtime_status/
`-- producer/
    `-- transaction_producer.py
```

## Prerequisites

1. Java is installed and available in PATH.
2. Apache Kafka is installed locally.
3. Apache Spark is installed locally (this setup uses Spark 3.5.8).
4. Python virtual environment exists at `.venv` in the project root.

## Step-by-Step Runbook

Use separate terminals for Kafka, Producer, Spark Consumer, and Streamlit.

### Step 0: Install Python dependencies (one-time)

From project root:

```powershell
c:/Users/jalle/VSC/kafka/.venv/Scripts/Activate.ps1
c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe -m pip install -r requirements.txt
```

Optional model training before streaming:

```powershell
c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe graph/graph_analysis.py --input data/sample_transactions.json
```

### Step 1: Initialize Kafka KRaft storage (first-time only)

Run these only if Kafka fails with `No readable meta.properties files found`.

```powershell
cd C:\testkafka\kafka_2.13-4.2.0\bin\windows
for /f %i in ('.\kafka-storage.bat random-uuid') do set CLUSTER_ID=%i
.\kafka-storage.bat format -t %CLUSTER_ID% -c ..\..\config\server.properties
```

### Step 2: Start Kafka broker

```powershell
cd C:\testkafka\kafka_2.13-4.2.0\bin\windows
.\kafka-server-start.bat ..\..\config\server.properties
```

From project terminal, verify broker reachability:

```powershell
Test-NetConnection -ComputerName 127.0.0.1 -Port 9092
```

Expected: `TcpTestSucceeded : True`

### Step 3: Start Producer

```powershell
C:\Users\jalle\VSC\kafka\.venv\Scripts\python.exe C:\Users\jalle\VSC\kafka\producer\transaction_producer.py
```

### Step 4: Start Spark Consumer

```powershell
cd C:\Users\jalle\VSC\kafka
$env:PYSPARK_PYTHON="c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe"
$env:PYSPARK_DRIVER_PYTHON="c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 --conf spark.pyspark.python=c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe --conf spark.pyspark.driver.python=c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe consumer/spark_stream_processor.py
```

### Step 5: Start Streamlit Dashboard

```powershell
cd C:\Users\jalle\VSC\kafka
c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe -m streamlit run app_ui.py
```

## Optional Neo4j Integration

### Mode A: Real-time sink from Spark

Set before starting Spark consumer:

```powershell
$env:NEO4J_SINK_ENABLED="true"
$env:NEO4J_URI="bolt://localhost:7687"
$env:NEO4J_USER="neo4j"
$env:NEO4J_PASSWORD="<your_neo4j_password>"
$env:NEO4J_DATABASE="neo4j"
```

### Mode B: Manual sink from flagged CSV

```powershell
c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe neo4j_sink.py --input output/flagged_cases/flagged_cases.csv --password <your_neo4j_password>
```

## Fast Demo Mode (optional)

To trigger fraud bursts faster during demo, set in producer terminal before running producer:

```powershell
$env:CYCLE_SECONDS="60"
$env:FRAUD_BURST_START="20"
$env:FRAUD_BURST_END="25"
```

## Quick Health Checks

Run from project terminal:

```powershell
Test-NetConnection -ComputerName 127.0.0.1 -Port 9092
c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe -c "from kafka import KafkaAdminClient; a=KafkaAdminClient(bootstrap_servers='127.0.0.1:9092'); print(sorted(list(a.list_topics()))[:10]); a.close()"
```

## Common Issues and Fixes

### `kafka.errors.NoBrokersAvailable`

1. Kafka is not running or not reachable.
2. Verify port with `Test-NetConnection -ComputerName 127.0.0.1 -Port 9092`.
3. Ensure `config/kafka_config.py` uses `127.0.0.1:9092`.

### `No readable meta.properties files found`

Kafka KRaft storage is not initialized. Run Step 1 once, then start Kafka again.

### `can't open file ... transaction_producer.py`

Use full path command from Step 3, or run from `producer` folder.

### `ModuleNotFoundError: No module named sklearn`

1. Install dependencies via Step 0.
2. Ensure Spark consumer is started with `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` set to `.venv` Python.

## Demo Checklist

1. Kafka port 9092 is reachable.
2. Producer is sending transactions.
3. Spark consumer is processing micro-batches.
4. Streamlit shows live metrics and analytics pages.
5. Flagged loops appear in Analytics/Graph pages after fraud bursts.


cd "C:\testkafka\kafka_2.13-4.2.0\bin\windows"
set CLUSTER_ID=TbO82k_ERPKT2nHGujsGDg
echo %CLUSTER_ID%
.\kafka-server-start.bat ..\..\config\server.properties
Test-NetConnection -ComputerName 127.0.0.1 -Port 9092
spark-shell
pyspark

C:\Users\jalle\VSC\kafka\.venv\Scripts\python.exe C:\Users\jalle\VSC\kafka\producer\transaction_producer.py

$env:PYSPARK_PYTHON="c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe"
>> $env:PYSPARK_DRIVER_PYTHON="c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe"
>> spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 --conf spark.pyspark.python=c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe --conf spark.pyspark.driver.python=c:/Users/jalle/VSC/kafka/.venv/Scripts/python.exe consumer/spark_stream_processor.py

treamlit run app_ui.py