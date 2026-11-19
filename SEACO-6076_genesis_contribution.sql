-- Databricks notebook source
-- SEACO-6076 : Genesis AR Incremental - contribution analysis
-- Run on the PROD ARC Databricks workspace (the one that runs job "Incremental_New_Model_E2E_Prod").
-- Read-only: every statement is a SELECT / DESCRIBE.
--
-- Tables (from orbis-range-bulk-pipeline/arc-core, SparkHelper.goldTable / inputTable):
--   processed_data_incremental.gold_metadata                  -> 1 row per (roadId, side) in the LIVE AR snapshot,
--                                                                         carries sourceType/user of the WINNING range
--   processed_data_incremental.gold_incremental_jobs          -> 1 row per incremental run, otlAddressRangeCount
--                                                                         (NOT covered by the 90-day retention cleanup)
--   processed_data_incremental.gold_otl_feature_address_range -> raw genesis-incr output per run (90-day retention)
--
-- Provenance values:
--   genesis-incremental (OTL)  : sourceType = 'GENESIS', user = 'OTL'
--   ARC new model              : sourceType = 'ORBIS',   user = 'ARCBULKUSER'
--   Merge rule (ArcOtlRangeMerger): per road side ARC always wins; OTL only used where ARC has NO range on that side.

-- COMMAND ----------

select count(*) from test.snapshot_layer_83260_ar_prod_data

-- COMMAND ----------

-- Q0. Sanity: provenance values actually present
SELECT sourceType, user, count(*) AS sides
FROM processed_data_incremental.gold_metadata
GROUP BY sourceType, user
ORDER BY sides DESC;

-- COMMAND ----------

-- Q1. Total genesis contribution in the CURRENT snapshot (road sides + roads, % of total)
WITH m AS (
  SELECT *, CASE WHEN sourceType = 'GENESIS' THEN 'GENESIS' ELSE 'OTHER' END AS src
  FROM processed_data_incremental.gold_metadata
)
SELECT src,
       count(*)                                                   AS road_sides,
       count(DISTINCT roadId)                                     AS roads,
       round(100 * count(*) / sum(count(*)) OVER (), 3)           AS pct_of_sides
FROM m
GROUP BY src;

-- COMMAND ----------

-- Q1b. Same, per country (where does genesis matter at all?)
SELECT countryIso,
       count(*)                                                        AS total_sides,
       sum(CASE WHEN sourceType = 'GENESIS' THEN 1 ELSE 0 END)         AS genesis_sides,
       round(100 * avg(CASE WHEN sourceType = 'GENESIS' THEN 1 ELSE 0 END), 3) AS genesis_pct
FROM processed_data_incremental.gold_metadata
GROUP BY countryIso
HAVING genesis_sides > 0
ORDER BY genesis_sides DESC;

-- COMMAND ----------

-- Q2-Q4 (a). Genesis OUTPUT trend per month, last 12 months (pre-merge: what the pipeline produced)
WITH j AS (
  SELECT *, coalesce(to_date(date, 'dd-MM-yyyy'), to_date(date)) AS run_date
  FROM processed_data_incremental.gold_incremental_jobs
)
SELECT date_trunc('month', run_date)  AS month,
       count(*)                       AS runs,
       sum(otlAddressRangeCount)      AS otl_ranges_produced,
       sum(featureCount)              AS impacted_features
FROM j
WHERE run_date >= add_months(current_date(), -12)
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- Q2-Q4 (b). Roll-up into the three windows the ticket asks for
WITH j AS (
  SELECT *, coalesce(to_date(date, 'dd-MM-yyyy'), to_date(date)) AS run_date
  FROM processed_data_incremental.gold_incremental_jobs
)
SELECT
  sum(CASE WHEN run_date >= add_months(current_date(), -3)  THEN otlAddressRangeCount END) AS otl_ranges_3m,
  sum(CASE WHEN run_date >= add_months(current_date(), -6)  THEN otlAddressRangeCount END) AS otl_ranges_6m,
  sum(CASE WHEN run_date >= add_months(current_date(), -12) THEN otlAddressRangeCount END) AS otl_ranges_12m,
  min(run_date) AS first_run_on_record
FROM j;

-- COMMAND ----------

-- Q2-Q4 (c). Genesis sides that are LIVE in the snapshot, by month they were last written
-- (post-merge: what actually reached the product; last-touch date, an upsert overwrites it)
SELECT date_trunc('month', from_unixtime(lastUpdatedTime / 1000)) AS month,
       count(*) AS live_genesis_sides
FROM processed_data_incremental.gold_metadata
WHERE sourceType = 'GENESIS'
  AND from_unixtime(lastUpdatedTime / 1000) >= add_months(current_date(), -12)
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- Q5a. Utilisation, last 90 days: how much genesis output survives the merge (ARC wins otherwise)
WITH otl AS (
  SELECT DISTINCT runId, roadId, side
  FROM processed_data_incremental.gold_otl_feature_address_range
  WHERE (violations IS NULL OR trim(violations) = '')
),
live AS (
  SELECT roadId, side, lastUpdatedOnRunId
  FROM processed_data_incremental.gold_metadata
  WHERE sourceType = 'GENESIS'
)
SELECT count(*)                                                     AS otl_sides_produced_90d,
       count(l.roadId)                                              AS otl_sides_live_in_snapshot,
       round(100 * count(l.roadId) / count(*), 2)                   AS pct_used
FROM otl o
LEFT JOIN live l ON l.roadId = o.roadId AND l.side = o.side;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Q5b + Q5c. NEW vs UPDATE, from the Delta log of gold_metadata.
-- MAGIC # (DESCRIBE HISTORY cannot be used as a subquery in SQL, and VERSION AS OF needs literal versions,
-- MAGIC #  so this cell reads the history and picks the versions itself.)
-- MAGIC TBL = "processed_data_incremental.gold_metadata"
-- MAGIC N_RUNS = 20  # how many recent incremental MERGEs to classify for GENESIS (Q5c)
-- MAGIC
-- MAGIC merges = (spark.sql(f"DESCRIBE HISTORY {TBL}")
-- MAGIC          .filter("operation = 'MERGE'")
-- MAGIC          .orderBy("version", ascending=False)
-- MAGIC          .limit(60))
-- MAGIC
-- MAGIC # Q5b: all sources. inserted = side had no range before (new HNs); updated = side already had a range.
-- MAGIC display(merges.selectExpr(
-- MAGIC     "version", "timestamp",
-- MAGIC     "cast(operationMetrics['numTargetRowsInserted'] as bigint) AS inserted_sides",
-- MAGIC     "cast(operationMetrics['numTargetRowsUpdated']  as bigint) AS updated_sides",
-- MAGIC     "cast(operationMetrics['numTargetRowsDeleted']  as bigint) AS deleted_sides"))
-- MAGIC
-- MAGIC # Q5c: GENESIS only. Compare each MERGE version with the version just before it
-- MAGIC # (time travel only works while the older version has not been vacuumed).
-- MAGIC results, skipped = [], []
-- MAGIC for r in merges.limit(N_RUNS).collect():
-- MAGIC     v = r["version"]
-- MAGIC     try:
-- MAGIC         df = spark.sql(f"""
-- MAGIC           WITH after AS (
-- MAGIC             SELECT roadId, side FROM {TBL} VERSION AS OF {v}
-- MAGIC             WHERE sourceType = 'GENESIS'
-- MAGIC               AND lastUpdatedTime = (SELECT max(lastUpdatedTime) FROM {TBL} VERSION AS OF {v})
-- MAGIC           ),
-- MAGIC           before AS (SELECT roadId, side, sourceType FROM {TBL} VERSION AS OF {v - 1})
-- MAGIC           SELECT {v} AS version, CAST('{r["timestamp"]}' AS timestamp) AS merged_at,
-- MAGIC                  CASE WHEN b.roadId IS NULL         THEN 'NEW side (new HNs)'
-- MAGIC                       WHEN b.sourceType = 'GENESIS' THEN 'UPDATE of an existing genesis side'
-- MAGIC                       ELSE                               'REPLACED a non-genesis side' END AS kind,
-- MAGIC                  count(*) AS sides
-- MAGIC           FROM after a LEFT JOIN before b ON b.roadId = a.roadId AND b.side = a.side
-- MAGIC           GROUP BY 1, 2, 3""")
-- MAGIC         results.append(df)
-- MAGIC     except Exception as e:
-- MAGIC         skipped.append((v, str(e).splitlines()[0][:150]))
-- MAGIC
-- MAGIC if results:
-- MAGIC     from functools import reduce
-- MAGIC     per_run = reduce(lambda a, b: a.unionByName(b), results).cache()
-- MAGIC     display(per_run.orderBy("version", "kind"))
-- MAGIC     display(per_run.groupBy("kind").sum("sides").withColumnRenamed("sum(sides)", "sides_total"))
-- MAGIC print(f"classified {len(results)} MERGE versions, skipped {len(skipped)} (vacuumed/unreadable):", skipped[:5])

-- COMMAND ----------

-- Q5d. HN volume behind genesis output, last 90 days
SELECT count(*)                         AS otl_ranges,
       sum(size(lrHouseNumbers))        AS otl_house_numbers,
       count(DISTINCT roadId)           AS roads
FROM processed_data_incremental.gold_otl_feature_address_range
WHERE violations IS NULL OR trim(violations) = '';
