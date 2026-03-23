# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Databricks experimental workspace for TomTom's geospatial data analysis, focused on APT (Address Point) data management and rooftop metrics analysis. All work is done through Jupyter notebooks executed in Databricks.

## Project Context

- **Platform:** Databricks (notebooks run there, not locally)
- **Data:** TomTom address points (APTs), building footprints (BFP), and search logs
- **Catalogs used:** `preprocess_prod`, `preprocess_dev`, `my_database`, `dc_output`, `delete_retriggers`
- **DBFS mount paths:** `/mnt/opas/`, `/mnt/opas-prod/`, `/mnt/source-precheck/`

## Key Libraries

- **PySpark / Spark SQL** — distributed data processing
- **Apache Sedona** — spatial SQL operations (`ST_Intersects`, `ST_GeomFromText`, `ST_GeoHash`, `ST_GeomFromWKT`)
- **GeoPandas / Shapely** — local geospatial operations via `.toPandas()` + `gpd.sjoin()`
- **Delta Lake** — underlying storage format for catalog tables
- **TomTom internal Scala libs** — `orbis-addressing-bulk-apt-tools` (`OrbisElementRepository`, `LoadFreshSnapshotData`) used in mixed-language notebooks via `%scala` magic

## Architecture & Data Flow

Notebooks follow a consistent pipeline pattern:

1. **Load** — Read CSVs from DBFS/mounted storage or query Databricks catalog tables
2. **Transform** — Convert coordinate IDs to unsigned format, build WKT geometries from lat/lng
3. **Spatial analysis** — Sedona-based spatial joins and intersection checks
4. **Validate / Export** — Compare snapshots, flag anomalies, write results to CSV or catalog

### Common patterns across notebooks

- Coordinate IDs are stored as signed integers but must be cast to **unsigned 64-bit** for cross-dataset matching. Python pattern: `int(val) & ((1 << 64) - 1)`. Scala pattern: `java.lang.Long.toUnsignedString(signedLong)`. Final form: `"{layerId}_{unsigned_high}_{unsigned_low}"` (e.g. `14533_...`).
- WKT geometry strings are constructed from lat/lng columns before Sedona operations: `CONCAT('POINT(', lat, ' ', lng, ')')`
- Sedona requires `SedonaContext.create(spark)` before any spatial operations; Spark session is also configured with `KryoSerializer` and `SedonaKryoRegistrator`
- Large spatial joins use `repartition(1000)` or GeoHash-based partitioning (`repartition(250, expr("ST_GeoHash(geom, 5)"))`) to avoid shuffle bottlenecks
- Haversine distance UDFs are defined inline when precise distance calculations are needed
- APT snapshot tables follow the pattern: `my_database.apt_snapshot_revision_{revisionId}_{layerId}` (layerId for APTs is `14533`)
- BFP data has two sources: OSM (`preprocess_dev.layer_53801`, filtered by `building=yes` tag) and MCR (`preprocess_prod.bfp`, filtered by `licenseZone`)

## Notebook Index by Purpose

| Notebook | Purpose |
|---|---|
| `Find-Roof-Top-Metrics-On-Relocated-APT-CSV.ipynb` | Rooftop accuracy metrics for relocated APTs (most recent focus) |
| `Find-Roof-Top-Metrics-simple-approach.ipynb` | Simplified rooftop metrics approach |
| `Finding-apa-relocated-apts.ipynb` | Detects APTs that moved between two snapshots using `leftanti` join on `unsigned_id` |
| `find-changelets-for-apt-id.ipynb` | Looks up changelet history for a specific APT by unsigned ID (`dc_output` catalog) |
| `Find-Missed-apt-ids-from-previous-csv.ipynb` | Finds APT IDs present in a delete CSV but not yet deleted from layer data |
| `scoping-apt-with-suburb-and-place.ipynb` | Scopes APTs where both `addr:suburb` and `addr:place` tags exist (mixed Python/Scala) |
| `find-apt-ids-to-be-deleted.ipynb` | Identifies APTs marked for deletion |
| `APT-on-Aerodrome-check-result-analysis.ipynb` | Validates APTs incorrectly placed on aerodromes |
| `validate-duplicate-feature-ids.ipynb` | Detects duplicate feature IDs |
| `find-check-fail-apt-in-pre-opas.ipynb` | APTs that failed pre-OPAS checks |
| `Load-changelets-to-catalog.ipynb` | Ingests changelet data into Databricks catalog |
| `unmount-and-mount-again.ipynb` | Manages DBFS mount points |
