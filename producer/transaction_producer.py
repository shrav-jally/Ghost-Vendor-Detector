import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTION_TOPIC

fake = Faker()

# Demo cycle settings
TX_PER_SECOND = 100
CYCLE_SECONDS = 300  # 5 minutes
FRAUD_BURST_START = 285  # 4m 45s
FRAUD_BURST_END = 290    # 4m 50s

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


def _build_fraud_burst_transactions():
    burst = []
    now_ts = datetime.utcnow().isoformat() + "Z"
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
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return txn

def start_producing():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )
    
    print(f"Starting transaction producer. Sending to {TRANSACTION_TOPIC}...")
    try:
        while True:
            txn = generate_transaction()
            producer.send(TRANSACTION_TOPIC, txn)
            # print(f"Sent: {txn}") # Optional: uncomment for verbose output
            time.sleep(1 / TX_PER_SECOND)
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    start_producing()
