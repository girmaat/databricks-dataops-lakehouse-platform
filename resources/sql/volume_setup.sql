-- holds the prepared CSV deliveries, before Bronze ingestion is implemented.

USE CATALOG adb_classic_compute_catalog;
USE SCHEMA landing;

CREATE VOLUME IF NOT EXISTS source_deliveries
COMMENT 'raw source delivery files before Bronze ingestion';

SHOW VOLUMES IN adb_classic_compute_catalog.landing;

DESCRIBE VOLUME adb_classic_compute_catalog.landing.source_deliveries;

USE CATALOG adb_classic_compute_catalog;
USE SCHEMA bronze;

CREATE VOLUME IF NOT EXISTS ingestion_state
COMMENT 'Managed storage for Bronze Auto Loader schema history and checkpoint state';

-- Verify ----------

DESCRIBE VOLUME adb_classic_compute_catalog.bronze.ingestion_state;