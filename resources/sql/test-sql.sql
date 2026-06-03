USE CATALOG adb_classic_compute_catalog;

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE VOLUME IF NOT EXISTS monitoring.checkpoints;

SHOW VOLUMES IN adb_classic_compute_catalog.monitoring;