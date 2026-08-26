-- Day 9: Flink checkpointing and fault tolerance

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.dir' = 'file:///home/anshika/distributed-video-streaming/flink/checkpoints';

SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '3';
SET 'restart-strategy.fixed-delay.delay' = '10s';

CREATE TABLE day9_source (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    data STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'video_chunks',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'day9-checkpoint-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE day9_recovery_output (
    video_id STRING,
    chunk_id INT,
    filename STRING,
    chunk_size_bytes BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'day9_recovery_output',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

INSERT INTO day9_recovery_output
SELECT
    video_id,
    chunk_id,
    filename,
    CAST(CHAR_LENGTH(data) / 2 AS BIGINT)
FROM day9_source;

