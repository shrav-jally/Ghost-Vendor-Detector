import sys
import os
import shutil
import time
import json
from datetime import datetime, timezone
import joblib
import networkx as nx
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTION_TOPIC
from graph.graph_analysis import (
    analyze_and_flag_graph,
    build_transaction_features,
    ANOMALY_MODEL_PATH,
)
from neo4j_sink import ingest_flagged_loops_to_neo4j

# Global persistent graph
global_graph = nx.DiGraph()
anomaly_model_bundle = None

# Cleanup prior state
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "flagged_cases")
CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "checkpoint")
RUNTIME_STATUS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "runtime_status")
CONSUMER_STATUS_PATH = os.path.join(RUNTIME_STATUS_DIR, "consumer_status.json")


def _env_flag(name, default="false"):
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "y", "on"}


NEO4J_SINK_ENABLED = _env_flag("NEO4J_SINK_ENABLED", "false")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
NEO4J_SINK_MAX_RETRIES = int(os.getenv("NEO4J_SINK_MAX_RETRIES", "5"))
NEO4J_SINK_BACKOFF_BASE_SECONDS = float(os.getenv("NEO4J_SINK_BACKOFF_BASE_SECONDS", "2"))
NEO4J_SINK_BACKOFF_MAX_SECONDS = float(os.getenv("NEO4J_SINK_BACKOFF_MAX_SECONDS", "60"))
NEO4J_SINK_MAX_QUEUE_ITEMS = int(os.getenv("NEO4J_SINK_MAX_QUEUE_ITEMS", "50"))
neo4j_retry_queue = []


def _utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_consumer_status(status, last_batch_id=None, rows_in_batch=0, flagged_rows=0, note=""):
    os.makedirs(RUNTIME_STATUS_DIR, exist_ok=True)
    payload = {
        "process": "spark_consumer",
        "status": status,
        "topic": TRANSACTION_TOPIC,
        "model_loaded": bool(anomaly_model_bundle is not None),
        "neo4j_sink_enabled": bool(NEO4J_SINK_ENABLED),
        "neo4j_retry_queue_size": int(len(neo4j_retry_queue)),
        "last_batch_id": None if last_batch_id is None else int(last_batch_id),
        "rows_in_batch": int(rows_in_batch),
        "flagged_rows": int(flagged_rows),
        "last_event": note,
        "last_heartbeat": _utc_now(),
    }
    with open(CONSUMER_STATUS_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

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


def get_or_load_anomaly_model():
    global anomaly_model_bundle
    if anomaly_model_bundle is not None:
        return anomaly_model_bundle

    if not os.path.exists(ANOMALY_MODEL_PATH):
        print(f"ML model not found at {ANOMALY_MODEL_PATH}. Falling back to heuristic confidence score.")
        anomaly_model_bundle = None
        _write_consumer_status("running", note="ML model unavailable; heuristic scoring active")
        return None

    anomaly_model_bundle = joblib.load(ANOMALY_MODEL_PATH)
    print(f"Loaded anomaly model from: {ANOMALY_MODEL_PATH}")
    _write_consumer_status("running", note=f"ML model loaded from {ANOMALY_MODEL_PATH}")
    return anomaly_model_bundle


def score_batch_with_anomaly_model(pdf):
    model_bundle = get_or_load_anomaly_model()
    if model_bundle is None:
        return add_confidence_score(pdf)

    model = model_bundle.get("model")
    feature_columns = model_bundle.get("feature_columns", ["amount", "pair_frequency", "sender_receiver_centrality"])
    if model is None:
        return add_confidence_score(pdf)

    # Create a temporary graph view that includes this micro-batch before cycle logic runs.
    feature_graph = global_graph.copy()
    for _, row in pdf.iterrows():
        feature_graph.add_edge(str(row["from_id"]), str(row["to_id"]))

    feature_pdf = build_transaction_features(pdf, graph=feature_graph)
    x_batch = feature_pdf[feature_columns].fillna(0.0)

    anomaly_flags = model.predict(x_batch)  # -1 = anomaly, 1 = normal
    anomaly_scores = -model.decision_function(x_batch)

    feature_pdf["is_anomaly"] = (anomaly_flags == -1).astype(int)
    min_score = float(anomaly_scores.min()) if len(anomaly_scores) else 0.0
    max_score = float(anomaly_scores.max()) if len(anomaly_scores) else 1.0
    denom = (max_score - min_score) if max_score > min_score else 1.0
    normalized = ((anomaly_scores - min_score) / denom) * 100.0
    feature_pdf["risk_score"] = pd.Series(normalized, index=feature_pdf.index).round(2)
    feature_pdf["confidence_score"] = feature_pdf["risk_score"]

    return feature_pdf


def sink_flagged_loops_if_enabled(flagged_pdf, epoch_id):
    global neo4j_retry_queue

    if flagged_pdf is None or flagged_pdf.empty:
        flagged_pdf = None

    if not NEO4J_SINK_ENABLED:
        _write_consumer_status("running", last_batch_id=epoch_id, rows_in_batch=0, flagged_rows=0, note="Neo4j sink disabled")
        return

    if not NEO4J_PASSWORD:
        print("Neo4j sink is enabled but NEO4J_PASSWORD is not set. Skipping sink for this batch.")
        _write_consumer_status("warning", last_batch_id=epoch_id, rows_in_batch=0, flagged_rows=0, note="Neo4j password missing")
        return

    current_time = time.time()

    # Try due retries first, so backlog is drained before new payloads.
    due_retries = [item for item in neo4j_retry_queue if item["next_retry_at"] <= current_time]
    deferred_retries = [item for item in neo4j_retry_queue if item["next_retry_at"] > current_time]
    neo4j_retry_queue = deferred_retries

    sink_payloads = due_retries
    if flagged_pdf is not None and not flagged_pdf.empty:
        sink_payloads.append(
            {
                "batch_id": int(epoch_id),
                "flagged_pdf": flagged_pdf.copy(),
                "attempt_count": 0,
                "next_retry_at": current_time,
            }
        )

    for payload in sink_payloads:
        batch_id = payload["batch_id"]
        payload_df = payload["flagged_pdf"]
        attempt_count = int(payload.get("attempt_count", 0)) + 1

        try:
            ingest_flagged_loops_to_neo4j(
                payload_df,
                uri=NEO4J_URI,
                user=NEO4J_USER,
                password=NEO4J_PASSWORD,
                database=NEO4J_DATABASE,
            )
            print(
                f"Neo4j sink completed for batch {batch_id} "
                f"({len(payload_df)} flagged rows, attempt {attempt_count})."
            )
            _write_consumer_status(
                "running",
                last_batch_id=batch_id,
                rows_in_batch=len(payload_df),
                flagged_rows=len(payload_df),
                note=f"Neo4j sink success on attempt {attempt_count}",
            )
        except Exception as exc:
            if attempt_count >= NEO4J_SINK_MAX_RETRIES:
                print(
                    f"Neo4j sink dropped batch {batch_id} after {attempt_count} attempts: {exc}"
                )
                _write_consumer_status(
                    "warning",
                    last_batch_id=batch_id,
                    rows_in_batch=len(payload_df),
                    flagged_rows=len(payload_df),
                    note=f"Neo4j sink dropped payload after {attempt_count} attempts",
                )
                continue

            if len(neo4j_retry_queue) >= NEO4J_SINK_MAX_QUEUE_ITEMS:
                dropped = neo4j_retry_queue.pop(0)
                print(
                    f"Neo4j retry queue full. Dropping oldest batch {dropped['batch_id']} "
                    f"to enqueue batch {batch_id}."
                )

            backoff_seconds = min(
                NEO4J_SINK_BACKOFF_BASE_SECONDS * (2 ** (attempt_count - 1)),
                NEO4J_SINK_BACKOFF_MAX_SECONDS,
            )
            neo4j_retry_queue.append(
                {
                    "batch_id": batch_id,
                    "flagged_pdf": payload_df,
                    "attempt_count": attempt_count,
                    "next_retry_at": time.time() + backoff_seconds,
                }
            )
            print(
                f"Neo4j sink failed for batch {batch_id} (attempt {attempt_count}): {exc}. "
                f"Retry in {backoff_seconds:.1f}s. Queue size={len(neo4j_retry_queue)}"
            )
            _write_consumer_status(
                "warning",
                last_batch_id=batch_id,
                rows_in_batch=len(payload_df),
                flagged_rows=len(payload_df),
                note=f"Neo4j sink retry queued (attempt {attempt_count})",
            )

def process_microbatch(df, epoch_id):
    global global_graph
    current_batch_pdf = df.toPandas()
    _write_consumer_status(
        "running",
        last_batch_id=epoch_id,
        rows_in_batch=len(current_batch_pdf),
        flagged_rows=0,
        note="Spark micro-batch received",
    )
    
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

        # Score every transaction in the micro-batch before cycle detection.
        analyzed_pdf = score_batch_with_anomaly_model(current_batch_pdf)
        flagged_pdf = analyze_and_flag_graph(global_graph, analyzed_pdf, epoch_id)
        sink_flagged_loops_if_enabled(flagged_pdf, epoch_id)
        _write_consumer_status(
            "running",
            last_batch_id=epoch_id,
            rows_in_batch=len(current_batch_pdf),
            flagged_rows=0 if flagged_pdf is None else len(flagged_pdf),
            note="Spark micro-batch processed",
        )

def process_stream():
    clear_previous_state()
    _write_consumer_status("starting", note="Initializing Spark session")
    
    spark = SparkSession.builder \
        .appName("GhostVendorDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8") \
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

    _write_consumer_status("running", note="Spark stream started and awaiting batches")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
