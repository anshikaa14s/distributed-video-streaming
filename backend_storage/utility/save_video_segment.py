from pathlib import Path
from backend_storage.config import conf


def save_video_segment(stream_id, quality, segment_name, segment_data):
    if quality not in conf.QUALITIES:
        raise ValueError(f"Unsupported quality: {quality}")

    output_dir = (
        Path(conf.DIRECTORIES["streams"])
        / f"stream_{stream_id}"
        / quality
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / segment_name

    with open(output_path, "wb") as f:
        f.write(segment_data)

    return output_path


if __name__ == "__main__":
    test_data = b"test segment data"

    path = save_video_segment(
        "test_video",
        "360p",
        "segment_000.ts",
        test_data
    )

    print(f"Saved: {path}")
