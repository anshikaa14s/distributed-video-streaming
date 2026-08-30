from pathlib import Path
from backend_storage.config import conf


def update_m3u8_file(
    stream_id,
    quality,
    segment_name,
    duration=2.0
):
    if quality not in conf.QUALITIES:
        raise ValueError(f"Unsupported quality: {quality}")

    metadata_dir = (
        Path(conf.DIRECTORIES["metadata"])
        / f"stream_{stream_id}"
        / quality
    )

    metadata_dir.mkdir(parents=True, exist_ok=True)

    playlist_path = metadata_dir / f"{quality}.m3u8"

    if not playlist_path.exists():
        playlist_path.write_text(
            "#EXTM3U\n"
            "#EXT-X-VERSION:3\n"
            "#EXT-X-TARGETDURATION:7\n"
            "#EXT-X-MEDIA-SEQUENCE:0\n"
        )

    with open(playlist_path, "a") as f:
        f.write(f"#EXTINF:{duration:.3f},\n")
        f.write(f"{segment_name}\n")

    return playlist_path


if __name__ == "__main__":
    path = update_m3u8_file(
        "test_video",
        "360p",
        "segment_000.ts",
        2.0
    )

    print(f"Updated playlist: {path}")
