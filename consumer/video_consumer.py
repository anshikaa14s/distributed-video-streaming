from kafka import KafkaConsumer
from pathlib import Path
import json

KAFKA_SERVER = "localhost:9092"
TOPIC = "video_chunks"

OUTPUT_DIR = Path("data/received_chunks")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id="day3-clean-group",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Waiting for video chunks...")


for message in consumer:

    data = message.value

    video_id = data["video_id"]
    chunk_id = data["chunk_id"]
    filename = data["filename"]

    chunk_data = bytes.fromhex(data["data"])

    output_path = OUTPUT_DIR / f"received_{filename}"

    with open(output_path, "wb") as f:
        f.write(chunk_data)

    print(
        f"Received: {filename} | "
        f"Video: {video_id} | "
        f"Chunk ID: {chunk_id}"
    )
