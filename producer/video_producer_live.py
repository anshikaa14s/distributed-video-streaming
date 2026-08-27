from kafka import KafkaProducer
from pathlib import Path
import json
import time
import zlib

KAFKA_SERVER = "localhost:9092"
TOPIC = "video_chunks"

CHUNK_DIR = Path("data/live_chunks")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sent_segments = set()

print("Live video producer started...")
print(f"Watching: {CHUNK_DIR}")

while True:

    segments = sorted(CHUNK_DIR.glob("segment_*.ts"))

    for segment_path in segments:

        if segment_path.name in sent_segments:
            continue

        # Ignore files that are still being written
        size_1 = segment_path.stat().st_size

        if size_1 == 0:
            continue

        time.sleep(0.2)

        if not segment_path.exists():
            continue

        size_2 = segment_path.stat().st_size

        if size_1 != size_2:
            continue

        with open(segment_path, "rb") as f:
            raw_data = f.read()

        compressed_data = zlib.compress(raw_data)

        segment_number = int(
            segment_path.stem.split("_")[-1]
        )

        message = {
            "video_id": "test_video",
            "chunk_id": segment_number,
            "filename": segment_path.name,
            "timestamp": time.time_ns(),
            "original_size": len(raw_data),
            "compressed_size": len(compressed_data),
            "data": compressed_data.hex()
        }

        producer.send(
            TOPIC,
            key=f"test_video_{segment_number}",
            value=message
        ).get()

        producer.flush()

        sent_segments.add(segment_path.name)

        print(
            f"Sent: {segment_path.name} | "
            f"Original: {len(raw_data)} bytes | "
            f"Compressed: {len(compressed_data)} bytes"
        )

    time.sleep(0.2)
