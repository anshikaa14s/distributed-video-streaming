CREATE TABLE video_stream (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    data STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'video_chunks',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-day7-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE video_chunk_analysis (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    chunk_size_bytes BIGINT,
    size_category STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'video_chunk_analysis',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

INSERT INTO video_chunk_analysis
SELECT
    video_id,
    chunk_id,
    filename,
    CAST(CHAR_LENGTH(data) / 2 AS BIGINT) AS chunk_size_bytes,
    CASE
        WHEN CHAR_LENGTH(data) / 2 >= 50000 THEN 'LARGE'
        ELSE 'SMALL'
    END AS size_category
FROM video_stream;

