import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTION_TOPIC

fake = Faker()

# Demo cycle settings
TX_PER_SECOND = 100
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "300"))
FRAUD_BURST_START = int(os.getenv("FRAUD_BURST_START", "285"))
FRAUD_BURST_END = int(os.getenv("FRAUD_BURST_END", "290"))

# Pool of 500 names for clean traffic
EMPLOYEES = [f"EMP_{fake.first_name().upper()}_{i}" for i in range(250)]
VENDORS = [f"VEND_{fake.company().upper().replace(' ', '_')}_{i}" for i in range(250)]

cycle_start_time = time.time()
burst_injected_for_cycle = False
fraud_queue = []

FRAUD_LOOPS = [
    ["EMP_FRAUD_A", "VEND_SHELL_A", "SUB_PAYBACK_A"],
    ["EMP_FRAUD_B", "VEND_SHELL_B", "SUB_PAYBACK_B"],
    ["EMP_FRAUD_C", "VEND_SHELL_C", "SUB_PAYBACK_C"],
]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNTIME_STATUS_DIR = os.path.join(BASE_DIR, "output", "runtime_status")
PRODUCER_STATUS_PATH = os.path.join(RUNTIME_STATUS_DIR, "producer_status.json")


def _utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_producer_status(status, tx_sent_total, fraud_queue_depth, note):
    os.makedirs(RUNTIME_STATUS_DIR, exist_ok=True)
    payload = {
        "process": "producer",
        "status": status,
        "topic": TRANSACTION_TOPIC,
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "tx_per_second_target": TX_PER_SECOND,
        "tx_sent_total": int(tx_sent_total),
        "fraud_queue_depth": int(fraud_queue_depth),
        "last_event": note,
        "last_heartbeat": _utc_now(),
    }
    with open(PRODUCER_STATUS_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _build_fraud_burst_transactions():
    burst = []
    now_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    for loop_nodes in FRAUD_LOOPS:
        for i in range(len(loop_nodes)):
            source_id = loop_nodes[i]
            target_id = loop_nodes[(i + 1) % len(loop_nodes)]
            burst.append(
                {
                    "from_id": source_id,
                    "to_id": target_id,
                    "amount": round(random.uniform(8500, 14500), 2),
                    "timestamp": now_ts,
                }
            )
    return burst

def generate_transaction():
    global cycle_start_time, burst_injected_for_cycle, fraud_queue

    # If we have a queued fraud transaction, send it immediately
    if fraud_queue:
        return fraud_queue.pop(0)

    current_time = time.time()
    elapsed_in_cycle = current_time - cycle_start_time

    # Start a new 5-minute cycle
    if elapsed_in_cycle >= CYCLE_SECONDS:
        cycle_start_time = current_time
        burst_injected_for_cycle = False
        elapsed_in_cycle = 0

    # Inject exactly one fraud burst between 4:45 and 4:50 of each cycle
    if (not burst_injected_for_cycle) and (FRAUD_BURST_START <= elapsed_in_cycle <= FRAUD_BURST_END):
        print("--- [DEMO ALERT] INJECTING FRAUD BURST NOW ---")
        fraud_queue = _build_fraud_burst_transactions()
        burst_injected_for_cycle = True
        return fraud_queue.pop(0)

    # Regular, clean, non-loop transaction
    from_id = random.choice(EMPLOYEES)
    to_id = random.choice(VENDORS)
    txn = {
        "from_id": from_id,
        "to_id": to_id,
        "amount": round(random.uniform(100, 5000), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    }
    return txn

def start_producing():
    tx_sent_total = 0
    last_status_write = 0.0

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        )
    except NoBrokersAvailable:
        message = (
            f"No Kafka broker available at {KAFKA_BOOTSTRAP_SERVERS}. "
            "Start Kafka and verify port 9092 before running producer."
        )
        print(message)
        _write_producer_status("error", tx_sent_total, len(fraud_queue), message)
        return

    print(f"Starting transaction producer. Sending to {TRANSACTION_TOPIC}...")
    _write_producer_status("starting", tx_sent_total, len(fraud_queue), "Bootstrapping Kafka producer")
    try:
        while True:
            txn = generate_transaction()
            producer.send(TRANSACTION_TOPIC, txn)
            tx_sent_total += 1

            current = time.time()
            if current - last_status_write >= 1.0:
                _write_producer_status("running", tx_sent_total, len(fraud_queue), "Streaming transactions")
                last_status_write = current

            # print(f"Sent: {txn}") # Optional: uncomment for verbose output
            time.sleep(1 / TX_PER_SECOND)
    except KeyboardInterrupt:
        print("Stopping producer.")
        _write_producer_status("stopped", tx_sent_total, len(fraud_queue), "Stopped by user")
    except Exception as exc:
        _write_producer_status("error", tx_sent_total, len(fraud_queue), f"Producer error: {exc}")
        raise
    finally:
        producer.close()

if __name__ == "__main__":
    start_producing()
