import sys
import os
import shutil
import networkx as nx
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTION_TOPIC
from graph.graph_analysis import analyze_and_flag_graph

# Global persistent graph
global_graph = nx.DiGraph()

# Cleanup prior state
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "flagged_cases")
CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "checkpoint")

def clear_previous_state():
    print("Clearing previous state...")
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)

def add_confidence_score(pdf):
    scored_pdf = pdf.copy()
    if scored_pdf.empty:
        scored_pdf["confidence_score"] = []
        return scored_pdf

    min_amount = 100.0
    max_amount = 5000.0
    score = ((scored_pdf["amount"] - min_amount) / (max_amount - min_amount)) * 100.0
    scored_pdf["confidence_score"] = score.clip(lower=0, upper=100).round(2)
    return scored_pdf

def process_microbatch(df, epoch_id):
    global global_graph
    current_batch_pdf = df.toPandas()
    
    if not current_batch_pdf.empty:
        # Add new edges to the global graph
        for _, row in current_batch_pdf.iterrows():
            global_graph.add_edge(row['from_id'], row['to_id'], amount=row['amount'], timestamp=row['timestamp'])

        print('DEBUG: Checking graph for cycles...')
        raw_cycles = list(nx.simple_cycles(global_graph))
        if raw_cycles:
            # We filter out large cycles here as debugging noise if necessary, or just trace them
            print(f"DEBUG: nx.simple_cycles found {len(raw_cycles)} cycles!")
            for c in raw_cycles:
                if len(c) <= 5: # Only print reasonable loops
                    print(f"DEBUG CYCLE (len={len(c)}): {' -> '.join(c)} -> {c[0]}")

        # The analysis function now needs the graph and the original dataframe for metadata
        analyzed_pdf = add_confidence_score(current_batch_pdf)
        analyze_and_flag_graph(global_graph, analyzed_pdf, epoch_id)

def process_stream():
    clear_previous_state()
    
    spark = SparkSession.builder \
        .appName("GhostVendorDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("from_id", StringType(), True),
        StructField("to_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TRANSACTION_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    query = parsed_df.writeStream \
        .foreachBatch(process_microbatch) \
        .outputMode("update") \
        .trigger(processingTime='5 seconds') \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
