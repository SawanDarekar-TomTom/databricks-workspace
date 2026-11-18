# SEACO-6161: DBFS root storage investigation and cleanup

Notebooks that find what fills the two workspace-managed DBFS root storage accounts, and shrink them.
Together the accounts cost about $3,235/month. Neither account accepts Azure lifecycle policies or CLI access,
so every step runs inside Databricks.

| Folder | Workspace | Root storage account | Size / cost (May 2026) |
|---|---|---|---|
| `opas-prod/` | `dbw-opas-merone-pipeline-prod` | `dbstorageld3xv3o6mfr6o` | 188.41 TiB / $2,189 |
| `arc-prod/` | `dbw-arc-merone-pipeline-prod` | `dbstorage347sd45lirtsc` | 92.45 TiB / $1,046 |

Both folders hold the same notebooks. Only the workspace config in `00_config_and_helpers` and the known findings differ.
**Import each folder into its own workspace**, and keep its four notebooks together in one folder, because they
use `%run ./00_config_and_helpers`.

## Run order

| Notebook | Changes data? | Purpose |
|---|---|---|
| `00_config_and_helpers` | no | Workspace config and shared helpers. Loaded by the others, never run on its own |
| `01_investigation` | no (writes audit tables only) | Directory breakdown, table inventory, VACUUM overhead, shallow-clone check, log destinations, writer jobs, before-cost baseline, cleanup candidates |
| `02_cleanup` | **yes, one step per run, dry run by default** | `vacuum_dry_run`, `vacuum`, `set_retention_properties`, `drop_tables`, `delete_orphan_dirs`, `prune_cluster_logs` |
| `03_weekly_vacuum_job` | yes, once scheduled | Weekly VACUUM of active root tables. Fails the run when disk is still more than 2× live size. The job JSON is in the last cell |

Suggested flow:

1. Run `01_investigation`.
2. Review sections 3, 4 and 9 with the table owners.
3. In `02_cleanup`, run `vacuum_dry_run`, then `vacuum` in small batches.
4. Drop the tables the owners confirmed.
5. Schedule `03_weekly_vacuum_job`.
6. Run `01_investigation` again and compare `baseline` before and after.

## Safety built in

- Nothing in `02_cleanup` acts unless `dry_run = false` **and** `confirm_workspace` is exactly the workspace name.
- VACUUM never goes below 168 hours. It stops if `retentionDurationCheck` has been disabled.
- **Shallow-clone sources are never VACUUMed.** If a "backup" is a `SHALLOW CLONE`, a VACUUM on its source would break it.
  `01_investigation` section 5 finds these from the Delta file lists.
- A table written in the last 30 days is not dropped unless you set `allow_recent = true`.
- An orphan directory is deleted only if the latest investigation listed it, and only if it overlaps no table location.
- Every action, dry or real, is recorded in `storage_audit_seaco_6161.cleanup_log`.

## Requirements

- **Cluster:** Single User (dedicated) access mode. Shared mode blocks `spark._jvm` and DBFS root access.
- **Permissions:** workspace admin is best, because the jobs and clusters APIs only return what the caller can see.
  The user also needs ownership or MODIFY on the tables to VACUUM, ALTER or DROP them.
- **ARC workspace:** the investigator needs access first. Tom De Leu got a 403 in June 2026.

## Limits

- Notebooks can only reach the `root` container, which is `dbfs:/`. The `jobs`, `logs`, `ephemeral` and `meta`
  containers, and any soft-deleted blobs, are not visible. They appear as the "not visible" gap in section 8.
- `DESCRIBE HISTORY` only reaches back 30 days (`logRetentionDuration`). Use `last_modified` for staleness, not
  history length.
- `VACUUM … DRY RUN` lists at most 1,000 files. The size estimate is `overhead_bytes`: on-disk − live − `_delta_log`.
