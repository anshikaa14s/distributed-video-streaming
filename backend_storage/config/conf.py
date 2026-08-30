import os

# Local equivalent of the reference storage configuration
BASE_DIR = os.path.join(
    os.getcwd(),
    "data",
    "backend_storage"
)

DIRECTORIES = {
    "streams": os.path.join(BASE_DIR, "streams"),
    "metadata": os.path.join(BASE_DIR, "metadata"),
}

QUALITIES = [
    "1080p",
    "720p",
    "480p",
    "360p",
]

NUM_OF_VMS = 3

LOG_LEVEL = "INFO"

KAFKA_BROKER = "localhost:9092"

SYNC_INTERVAL = 60
MONITOR_INTERVAL = 10
MAX_RETRIES = 3
