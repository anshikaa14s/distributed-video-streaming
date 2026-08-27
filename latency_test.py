import time
import json
from pathlib import Path
from kafka import KafkaProducer, KafkaConsumer
import subprocess

SERVER = "localhost:9092"
TOPIC = "latency_test"

# Create topic
subprocess.run([
    "bash", "-c",
    "~/tools/kafka_2.13-4.3.1/bin/kafka-topics.sh "
    "--bootstrap-server localhost:9092 "
    "--create --if-not-exists --topic latency_test "
    "--partitions 1 --replication-factor 1"
], capture_output=True, text=True)

# Unique consumer group + earliest so no message is missed
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=SERVER,
    group_id=f"latency-test-{time.time_ns()}",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode())
)

# Force consumer assignment BEFORE producing
consumer.poll(timeout_ms=1000)

producer = KafkaProducer(
    bootstrap_servers=SERVER,
    value_serializer=lambda v: json.dumps(v).encode()
)

chunks = sorted(Path("data/chunks").glob("chunk_*.mp4"))

print("Starting latency test...\n")

results = []

for chunk in chunks:
    start = time.perf_counter_ns()

    producer.send(TOPIC, {
        "filename": chunk.name,
        "timestamp_ns": start
    }).get()

    while True:
        records = consumer.poll(timeout_ms=1000)

        for _, messages in records.items():
            for message in messages:
                if message.value["filename"] == chunk.name:
                    received = time.perf_counter_ns()
                    latency_ms = (received - start) / 1_000_000
                    results.append(latency_ms)

                    print(f"{chunk.name}: {latency_ms:.2f} ms")
                    break
            else:
                continue
            break
        else:
            continue
        break

producer.close()
consumer.close()

print("\nLatency Summary")
print("----------------")
print(f"Chunks tested : {len(results)}")
print(f"Average       : {sum(results)/len(results):.2f} ms")
print(f"Minimum       : {min(results):.2f} ms")
print(f"Maximum       : {max(results):.2f} ms")
