from kafka import KafkaProducer
from pathlib import Path
import json

KAFKA_SERVER = "localhost:9092"
TOPIC = "video_chunks"

CHUNK_DIR = Path("data/chunks")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

chunks = sorted(CHUNK_DIR.glob("chunk_*.mp4"))

for chunk_id, chunk_path in enumerate(chunks):

    with open(chunk_path, "rb") as f:
        chunk_data = f.read()

    message = {
        "video_id": "test_video",
        "chunk_id": chunk_id,
        "filename": chunk_path.name,
        "data": chunk_data.hex()
    }

    producer.send(
        TOPIC,
        value=message
    )

    print(f"Sent: {chunk_path.name}")

producer.flush()
producer.close()

print("All chunks sent successfully.")