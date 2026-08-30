from pathlib import Path
from backend_storage.config import conf


def create_stream_dirs(stream_id):
    stream_path = Path(conf.DIRECTORIES["streams"]) / f"stream_{stream_id}"
    metadata_path = Path(conf.DIRECTORIES["metadata"]) / f"stream_{stream_id}"

    for quality in conf.QUALITIES:
        (stream_path / quality).mkdir(parents=True, exist_ok=True)
        (metadata_path / quality).mkdir(parents=True, exist_ok=True)

        playlist = metadata_path / quality / f"{quality}.m3u8"

        if not playlist.exists():
            playlist.write_text(
                "#EXTM3U\n"
                "#EXT-X-VERSION:3\n"
                "#EXT-X-TARGETDURATION:7\n"
                "#EXT-X-MEDIA-SEQUENCE:0\n"
            )


if __name__ == "__main__":
    create_stream_dirs("test_video")
    print("Stream directories created.")
