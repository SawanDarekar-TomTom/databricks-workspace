# SEACO-6161: DBFS root storage investigation and cleanup

Notebooks that find what fills the two workspace-managed DBFS root storage accounts, and shrink them.
Together the accounts cost about $3,235/month. Neither account accepts Azure lifecycle policies or CLI access,
so every step runs inside Databricks.

| Folder | Workspace | Root storage account | Size / cost (May 2026) |
|---|---|---|---|
| `opas-prod/` | `dbw-opas-merone-pipeline-prod` | `dbstorageld3xv3o6mfr6o` | 188.41 TiB / $2,189 |
| `arc-prod/` | `dbw-arc-merone-pipeline-prod` | `dbstorage347sd45lirtsc` | 92.45 TiB / $1,046 |

Both folders hold the same notebooks. Only the workspace config in `00_config_and_helpers` and the known findings differ.
**Import each folder into its own workspace**, and keep its notebooks together in one folder, because they
use `%run ./00_config_and_helpers`.

## Run order

| Notebook | Changes data? | Purpose |
|---|---|---|
| `00_config_and_helpers` | no | Workspace config and shared helpers. Loaded by the others, never run on its own |
| `01_investigation` | no (writes audit tables only) | Table inventory, VACUUM overhead, shallow-clone check, log destinations, writer jobs, before-cost baseline, cleanup candidates. About 10–30 min |
| `01a_dbfs_scan` | no (writes audit tables only) | Size of every `dbfs:/` directory. Sizing runs on the executors in saved batches, within a time budget (`time_budget_min`). Resume an unfinished run with `resume_run_id`. Can run alongside `01` |
| `04_cost_report` | no | Reads the latest `01` and `01a` audit runs and turns them into $/month: non-overlapping cost buckets, most expensive directories, databases and tables with a suggested action, and a Markdown summary for the ticket. 1–2 min |
| `02_cleanup` | **yes, one step per run, dry run by default** | `vacuum_dry_run`, `vacuum`, `set_retention_properties`, `drop_tables`, `drop_databases`, `delete_orphan_dirs`, `prune_cluster_logs` |
| `03_monthly_vacuum_job` | only when re-run with `dry_run = false` | Monthly VACUUM (last day of the month) of root tables written in the last 35 days. The scheduled run is a **dry run**. After an engineer checks it, they re-run the job with `dry_run = false`. The real run fails when disk is still more than 2× live size. The job JSON is in the last cell |
| `table_counts` (folder root, not per workspace) | no | Standalone (no `%run`), import into either workspace. For each table in the chosen databases (`databases`, empty = all): database, type, format, row count, live size (`DESCRIBE DETAIL`, excludes old versions) and file count, plus totals per database. `scan_files = true` counts by reading the data files, useful before and after a VACUUM |
| `vacuum_cleanup` (folder root, not per workspace) | **yes, when `dry_run = false`** (dry run by default) | Generic VACUUM for the workspace it runs in, with no config and no `%run`. It lists every database and table with row count, live size, on-disk size and old-version overhead, skips shallow-clone sources, and VACUUMs the largest candidates. For each table it prints rows, size and files before and after, and it stops if the row count changes. The summary shows the GiB freed (estimated in a dry run) and how many databases and tables were freed. A real run needs `confirm_workspace` = the workspace URL |

Suggested flow:

1. Run `01_investigation`, and `01a_dbfs_scan` alongside it on a second cluster. Resume `01a` until coverage reaches 100%.
2. Run `04_cost_report` to see what costs money, then review it (and `01` sections 2 and 7) with the table owners.
3. In `02_cleanup`, run `vacuum_dry_run`, then `vacuum` in small batches.
4. Drop the tables and databases the owners confirmed (`drop_tables`, `drop_databases`).
5. Schedule `03_monthly_vacuum_job`. Each scheduled run is a dry run. Check its output, then re-run it with `dry_run = false`.
6. Run `01_investigation` again and compare `baseline` before and after.

## Safety built in

- Nothing in `02_cleanup` acts unless `dry_run = false` **and** `confirm_workspace` is exactly the workspace name.
- VACUUM never goes below 168 hours. It stops if `retentionDurationCheck` has been disabled.
- Every real VACUUM counts the table's rows before and after, on the same table version and by reading the data
  files (not the Delta statistics). If the counts differ or the table cannot be read, the step stops and logs
  `row_check_failed`.
- **Shallow-clone sources are never VACUUMed.** If a "backup" is a `SHALLOW CLONE`, a VACUUM on its source would break it.
  `01_investigation` section 3 finds these from the Delta file lists.
- A table written in the last 30 days is not dropped unless you set `allow_recent = true`.
- An orphan directory is deleted only if the latest investigation listed it, and only if it overlaps no table location.
- Every action, dry or real, is recorded in `storage_audit_seaco_6161.cleanup_log`, with bytes and file counts
  before and after (`files_before`, `files_after`). VACUUM also prints them per table.

## Requirements

- **Cluster:** Single User (dedicated) access mode. Shared mode blocks `spark._jvm` and DBFS root access.
- **Permissions:** workspace admin is best, because the jobs and clusters APIs only return what the caller can see.
  The user also needs ownership or MODIFY on the tables to VACUUM, ALTER or DROP them.
- **ARC workspace:** the investigator needs access first. Tom De Leu got a 403 in June 2026.

## Limits

- A full `01a_dbfs_scan` lists every file on the account, so its run time follows the file count, not the size.
  More workers make it faster. One directory with millions of files (a never-vacuumed table) is a single task and
  sets the floor. The `01a` "slowest directories" list shows which ones those are.

- Notebooks can only reach the `root` container, which is `dbfs:/`. The `jobs`, `logs`, `ephemeral` and `meta`
  containers, and any soft-deleted blobs, are not visible. They appear as the "not visible" gap in `01` section 6.
- `DESCRIBE HISTORY` only reaches back 30 days (`logRetentionDuration`). Use `last_modified` for staleness, not
  history length.
- `VACUUM … DRY RUN` lists at most 1,000 files. The size estimate is `overhead_bytes`: on-disk − live − `_delta_log`.
