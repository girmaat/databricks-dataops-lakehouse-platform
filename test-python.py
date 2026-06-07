base = "/Volumes/adb_classic_compute_catalog/landing/source_deliveries"

folders = [
    f"{base}/employee_directory_snapshot/delivery_date=2026-05-01/",
    f"{base}/license_assignment_change_event/delivery_date=2026-05-01/",
    f"{base}/application_usage_event/delivery_date=2026-05-15/",
    f"{base}/application_usage_event/delivery_date=2026-05-21/",
]

for folder in folders:
    dbutils.fs.mkdirs(folder)
    print(folder)