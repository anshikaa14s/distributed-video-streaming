from kafka import KafkaConsumer
from pathlib import Path
import json
import zlib

KAFKA_SERVER = "localhost:9092"
TOPIC = "video_chunks"

OUTPUT_DIR = Path("data/received_live_chunks")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id="live-video-consumer",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Waiting for live video segments...")

for message in consumer:

    data = message.value

    video_id = data["video_id"]
    chunk_id = data["chunk_id"]
    filename = data["filename"]

    compressed_data = bytes.fromhex(data["data"])
    chunk_data = zlib.decompress(compressed_data)

    output_path = OUTPUT_DIR / f"received_{filename}"

    with open(output_path, "wb") as f:
        f.write(chunk_data)

    print(
        f"Received: {filename} | "
        f"Chunk ID: {chunk_id} | "
        f"Original: {data['original_size']} bytes | "
        f"Compressed: {data['compressed_size']} bytes | "
        f"Output: {output_path}"
    )
