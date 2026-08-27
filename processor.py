from kafka import KafkaConsumer
from pathlib import Path
import json
import zlib
import subprocess
import time

KAFKA_SERVER = "localhost:9092"
TOPIC = "video_chunks"

BASE_DIR = Path("data/processed")

QUALITIES = {
    "1080p": "1920:1080",
    "720p": "1280:720",
    "480p": "854:480",
    "360p": "640:360",
}

for quality in QUALITIES:
    (BASE_DIR / quality).mkdir(parents=True, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id="video-processor",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Video processor started...")
print("Waiting for video segments...")

for message in consumer:

    data = message.value

    filename = data["filename"]
    chunk_id = data["chunk_id"]

    try:
        compressed_data = bytes.fromhex(data["data"])
        segment_data = zlib.decompress(compressed_data)

        temp_file = BASE_DIR / f"segment_{chunk_id:03d}.ts"

        with open(temp_file, "wb") as f:
            f.write(segment_data)

        print(
            f"Processing {filename} | "
            f"chunk={chunk_id}"
        )

        start = time.perf_counter()

        processes = []

        for quality, resolution in QUALITIES.items():

            output_file = (
                BASE_DIR
                / quality
                / f"segment_{chunk_id:03d}.mp4"
            )

            command = [
                "ffmpeg",
                "-y",
                "-i", str(temp_file),
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-vf", f"scale={resolution}",
                "-c:a", "aac",
                "-movflags", "+faststart",
                str(output_file)
            ]

            processes.append(
                (quality, subprocess.Popen(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                ))
            )

        for quality, process in processes:
            return_code = process.wait()

            if return_code == 0:
                print(
                    f"  ✓ {quality} complete"
                )
            else:
                print(
                    f"  ✗ {quality} failed"
                )

        elapsed = time.perf_counter() - start

        print(
            f"Chunk {chunk_id} processed in "
            f"{elapsed:.2f} seconds\n"
        )

        temp_file.unlink(missing_ok=True)

    except Exception as e:
        print(
            f"ERROR processing {filename}: {e}"
        )
