import argparse
import os
import json
from datetime import datetime, timezone

import pandas as pd
from neo4j import GraphDatabase


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_PATH = os.path.join(BASE_DIR, "output", "flagged_cases", "flagged_cases.csv")
RUNTIME_STATUS_DIR = os.path.join(BASE_DIR, "output", "runtime_status")
NEO4J_STATUS_PATH = os.path.join(RUNTIME_STATUS_DIR, "neo4j_sink_status.json")

REQUIRED_COLUMNS = {
    "batch_id",
    "loop_id",
    "step_order",
    "from_id",
    "to_id",
    "amount",
    "timestamp",
    "confidence_score",
    "audit_reason",
}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_neo4j_status(status, rows=0, note=""):
    os.makedirs(RUNTIME_STATUS_DIR, exist_ok=True)
    payload = {
        "process": "neo4j_sink",
        "status": status,
        "rows": int(rows),
        "last_event": note,
        "last_heartbeat": utc_now_iso(),
    }
    with open(NEO4J_STATUS_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def infer_entity_type(entity_id):
    entity = str(entity_id)
    if entity.startswith("EMP_"):
        return "Employee"
    if entity.startswith("VEND_"):
        return "Vendor"
    return "Unknown"


def load_flagged_loops(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Flagged loops file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    missing_columns = REQUIRED_COLUMNS.difference(set(df.columns))
    if missing_columns:
        raise ValueError(
            f"Input file is missing required columns: {sorted(missing_columns)}"
        )

    if df.empty:
        return df

    df["step_order"] = pd.to_numeric(df["step_order"], errors="coerce").fillna(0).astype(int)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce").fillna(0.0)
    return df


def ensure_constraints(driver, database):
    queries = [
        "CREATE CONSTRAINT loop_id_unique IF NOT EXISTS FOR (l:FraudLoop) REQUIRE l.loop_id IS UNIQUE",
        "CREATE CONSTRAINT entity_id_unique IF NOT EXISTS FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE",
    ]
    with driver.session(database=database) as session:
        for query in queries:
            session.run(query)


def upsert_flagged_transaction(tx, row, ingested_at):
    tx.run(
        """
        MERGE (src:Entity {entity_id: $from_id})
          ON CREATE SET src.entity_type = $from_type
          ON MATCH SET src.entity_type = coalesce(src.entity_type, $from_type)
        MERGE (dst:Entity {entity_id: $to_id})
          ON CREATE SET dst.entity_type = $to_type
          ON MATCH SET dst.entity_type = coalesce(dst.entity_type, $to_type)
        MERGE (loop:FraudLoop {loop_id: $loop_id})
          ON CREATE SET loop.created_at = $ingested_at
        SET loop.batch_id = $batch_id,
            loop.audit_reason = $audit_reason,
            loop.updated_at = $ingested_at
        MERGE (src)-[r:FLAGGED_TRANSFER {
            loop_id: $loop_id,
            batch_id: $batch_id,
            step_order: $step_order
        }]->(dst)
        SET r.amount = $amount,
            r.timestamp = $timestamp,
            r.confidence_score = $confidence_score,
            r.audit_reason = $audit_reason,
            r.ingested_at = $ingested_at
        MERGE (src)-[:PART_OF_LOOP {loop_id: $loop_id}]->(loop)
        MERGE (dst)-[:PART_OF_LOOP {loop_id: $loop_id}]->(loop)
        """,
        from_id=str(row["from_id"]),
        to_id=str(row["to_id"]),
        from_type=infer_entity_type(row["from_id"]),
        to_type=infer_entity_type(row["to_id"]),
        loop_id=str(row["loop_id"]),
        batch_id=str(row["batch_id"]),
        step_order=int(row["step_order"]),
        amount=float(row["amount"]),
        timestamp=str(row["timestamp"]),
        confidence_score=float(row["confidence_score"]),
        audit_reason=str(row["audit_reason"]),
        ingested_at=ingested_at,
    )


def ingest_flagged_loops_to_neo4j(df, uri, user, password, database):
    if df.empty:
        print("No flagged loops found. Nothing to ingest.")
        write_neo4j_status("idle", rows=0, note="No flagged loops to ingest")
        return

    write_neo4j_status("running", rows=len(df), note="Connecting and writing to Neo4j")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        ensure_constraints(driver, database)

        ingested_at = utc_now_iso()
        with driver.session(database=database) as session:
            for _, row in df.iterrows():
                session.execute_write(upsert_flagged_transaction, row, ingested_at)

        print(f"Ingested {len(df)} flagged loop steps into Neo4j ({database}).")
        write_neo4j_status("running", rows=len(df), note=f"Ingested into database {database}")
    except Exception as exc:
        write_neo4j_status("error", rows=len(df), note=f"Neo4j write error: {exc}")
        raise
    finally:
        driver.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest flagged fraud loops into Neo4j.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to flagged_cases.csv")
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"), help="Neo4j URI")
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"), help="Neo4j username")
    parser.add_argument("--password", default=os.getenv("NEO4J_PASSWORD"), help="Neo4j password")
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", "neo4j"), help="Neo4j database")
    args = parser.parse_args()

    if not args.password:
        raise ValueError("Neo4j password is required. Pass --password or set NEO4J_PASSWORD.")

    flagged_loops_df = load_flagged_loops(args.input)
    ingest_flagged_loops_to_neo4j(
        flagged_loops_df,
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.database,
    )


if __name__ == "__main__":
    main()
