-- Purpose:
--   Add hard Delta CHECK constraints to accepted/project-owned Silver tables so invalid accepted records cannot be silently written.

USE CATALOG adb_classic_compute_catalog;
USE SCHEMA silver;

-- ============================================================
-- Project-owned Silver tables
-- ============================================================

-- assignment_current table constraints

ALTER TABLE assignment_current
ADD CONSTRAINT assignment_current_required_keys
CHECK (
  assignment_id IS NOT NULL
  AND employee_id IS NOT NULL
);

ALTER TABLE assignment_current
ADD CONSTRAINT assignment_current_required_state_values
CHECK (
  application_id IS NOT NULL
  AND license_tier_id IS NOT NULL
  AND current_effective_timestamp IS NOT NULL
);

ALTER TABLE assignment_current
ADD CONSTRAINT assignment_current_powerbi_application
CHECK (
  application_id = 'APP-PBI'
);

ALTER TABLE assignment_current
ADD CONSTRAINT assignment_current_valid_license_tier
CHECK (
  license_tier_id IN ('PBI-PRO', 'PBI-PPU')
);

-- application_usage_event_deduplicated table constraints

ALTER TABLE application_usage_event_deduplicated
ADD CONSTRAINT usage_dedup_required_keys
CHECK (
  usage_event_id IS NOT NULL
  AND employee_id IS NOT NULL
);

ALTER TABLE application_usage_event_deduplicated
ADD CONSTRAINT usage_dedup_required_event_values
CHECK (
  application_id IS NOT NULL
  AND event_type_code IS NOT NULL
  AND event_timestamp_utc IS NOT NULL
);

ALTER TABLE application_usage_event_deduplicated
ADD CONSTRAINT usage_dedup_powerbi_application
CHECK (
  application_id = 'APP-PBI'
);

ALTER TABLE application_usage_event_deduplicated
ADD CONSTRAINT usage_dedup_non_negative_units
CHECK (
  event_units IS NULL OR event_units >= 0
);

ALTER TABLE application_usage_event_deduplicated
ADD CONSTRAINT usage_dedup_supported_event_type
CHECK (
  event_type_code IN (
    'REPORT_VIEWED',
    'DASHBOARD_SHARED',
    'DATA_MODEL_AUTHORED',
    'PAGINATED_REPORT_PUBLISHED',
    'SIGN_IN'
  )
);

-- application_usage_event_late_rejects table constraints

ALTER TABLE application_usage_event_late_rejects
ADD CONSTRAINT usage_late_reject_required_keys
CHECK (
  usage_event_id IS NOT NULL
  AND employee_id IS NOT NULL
);

ALTER TABLE application_usage_event_late_rejects
ADD CONSTRAINT usage_late_reject_required_values
CHECK (
  event_timestamp_utc IS NOT NULL
  AND late_reject_reason IS NOT NULL
);