CREATE DATABASE IF NOT EXISTS test_db;
CREATE TABLE IF NOT EXISTS test_db.cell_towers
(
    radio String,
    mcc UInt16,
    net UInt16,
    area UInt16,
    cell UInt32,
    unit Int16,
    lon Float64,
    lat Float64,
    range UInt16,
    samples UInt8,
    changeable UInt8,
    created DateTime('UTC'),
    updated DateTime('UTC'),
    averageSignal Int8
)
ENGINE = ReplacingMergeTree
ORDER BY (radio, mcc, net, area, cell);