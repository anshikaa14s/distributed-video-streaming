CREATE TABLE video_stream (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    data STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'video_chunks',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-video-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE processed_video_chunks (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    chunk_size_bytes BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'processed_video_chunks',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

INSERT INTO processed_video_chunks
SELECT
    video_id,
    chunk_id,
    filename,
    CAST(CHAR_LENGTH(data) / 2 AS BIGINT) AS chunk_size_bytes
FROM video_stream;
