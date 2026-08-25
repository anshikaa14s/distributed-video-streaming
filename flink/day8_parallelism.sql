CREATE TABLE day8_source (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    data STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'video_chunks',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'day8-parallel-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE day8_parallel_output (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    chunk_size_bytes BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'day8_parallel_output',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

INSERT INTO day8_parallel_output
SELECT
    video_id,
    chunk_id,
    filename,
    CAST(CHAR_LENGTH(data) / 2 AS BIGINT)
FROM day8_source;
