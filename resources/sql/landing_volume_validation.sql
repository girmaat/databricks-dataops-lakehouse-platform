--   Validate the real Unity Catalog managed Volume used as the governed landing area for raw delivered source files.

USE CATALOG adb_classic_compute_catalog;
USE SCHEMA landing;


-- 1. Confirm the landing schema contains the governed Volume.

SHOW VOLUMES IN adb_classic_compute_catalog.landing;


-- 2. Inspect the real landing Volume metadata.

DESCRIBE VOLUME adb_classic_compute_catalog.landing.source_deliveries;


-- 3. Confirm the top-level source delivery folders exist.

LIST '/Volumes/adb_classic_compute_catalog/landing/source_deliveries/';


-- 4. Confirm the employee snapshot delivery file exists.

LIST '/Volumes/adb_classic_compute_catalog/landing/source_deliveries/employee_directory_snapshot/delivery_date=2026-05-01/';


-- 5. Confirm the license assignment event delivery file exists.

LIST '/Volumes/adb_classic_compute_catalog/landing/source_deliveries/license_assignment_change_event/delivery_date=2026-05-01/';


-- 6. Confirm the application usage event delivery file exists.

LIST '/Volumes/adb_classic_compute_catalog/landing/source_deliveries/application_usage_event/delivery_date=2026-05-15/';



DESCRIBE TABLE adb_classic_compute_catalog.bronze.application_usage_event_raw;





