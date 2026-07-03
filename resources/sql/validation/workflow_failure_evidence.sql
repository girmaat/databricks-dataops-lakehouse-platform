SELECT
  current_timestamp() AS evidence_captured_at_utc,
  'assignment_usage_learning_job' AS workflow_name,
  'AT_LEAST_ONE_FAILED path was reached' AS evidence_reason,
  'One or more upstream workflow tasks failed, so this conditional evidence task ran.' AS interpretation;