from pathlib import Path

CHUNK_DIR = Path("data/chunks")

chunks = sorted(CHUNK_DIR.glob("chunk_*.mp4"))

for chunk_id, chunk in enumerate(chunks):
    print({
        "video_id": "test_video",
        "chunk_id": chunk_id,
        "file": str(chunk),
    })
