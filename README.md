## Day 5 – Kafka to Flink Streaming Integration

### Objective
Integrated Apache Kafka with Apache Flink to build a real-time video chunk processing pipeline.

### Architecture

Video Chunks
    ↓
Kafka Producer
    ↓
Kafka Topic: video_chunks
    ↓
Flink Kafka Connector
    ↓
Flink Streaming Table: video_stream
    ↓
Chunk Processing
    ↓
Kafka Topic: processed_video_chunks
    ↓
Kafka Consumer

### Implementation

Created a Flink SQL streaming table connected to the Kafka `video_chunks` topic.

Processed each video chunk by calculating its original byte size from the hexadecimal payload:

`chunk_size_bytes = CHAR_LENGTH(data) / 2`

The processed records were written to the `processed_video_chunks` Kafka topic.

### Sample Output

| Chunk | Size (bytes) |
|---|---:|
| chunk_000.mp4 | 69838 |
| chunk_001.mp4 | 38142 |
| chunk_002.mp4 | 38118 |
| chunk_003.mp4 | 37460 |

### Technologies
- Apache Kafka 4.3.1
- Apache Flink 2.1.0
- Flink Kafka SQL Connector
- Python
- Flink SQL


## Day 6 – Flink Parallelism

### Objective
Configured Apache Flink for parallel stream processing using Kafka partitions and multiple Flink task slots.

### Configuration

Kafka topic:

`video_chunks`

Number of Kafka partitions:

`3`

Flink TaskManager slots:

`3`

Flink default parallelism:

`3`

### Parallel Processing Architecture

Kafka
├── Partition 0 ──→ Flink Subtask 1
├── Partition 1 ──→ Flink Subtask 2
└── Partition 2 ──→ Flink Subtask 3

### Verification

The Flink Web UI confirmed:

- Job Type: STREAMING
- Job State: RUNNING
- Source Parallelism: 3
- Source Tasks: 3
- TaskManager Slots: 3

This demonstrates parallel processing of the Kafka stream using Apache Flink.

### Day 6 Result

Successfully demonstrated Kafka partition-based parallel stream processing with Flink.# distributed-video-streaming
Distributed low-latency video streaming system built with Apache Kafka, FFmpeg, and Apache Flink. Implements real-time video chunking, distributed stream processing, fault-tolerant data pipelines, and scalable multi-node media delivery with performance and latency monitoring.
