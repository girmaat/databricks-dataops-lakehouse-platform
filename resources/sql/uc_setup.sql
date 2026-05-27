USE CATALOG adb_classic_compute_catalog;

CREATE SCHEMA IF NOT EXISTS landing
COMMENT 'raw incoming enterprise license files';

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw Delta ingestion layer preserving delivered source records and ingestion metadata.';

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

CREATE SCHEMA IF NOT EXISTS governance
COMMENT 'Sensitive-data policies, restricted views and governance-related project assets.';

CREATE SCHEMA IF NOT EXISTS monitoring
COMMENT 'Data quality, reconciliation, operational audit and platform-cost monitoring outputs.';