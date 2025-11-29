# Voyage Summary Incremental Plan

This doc explains what the voyage summary produces, what changed from the prior full-scan approach, and how the current windowed/stateful process runs.

## What the summary is
- Grain: one row per voyage (`MMSI`, `VoyageID`) covering the full trip (first to last AIS ping).
- Metrics: `VoyageStart`, `VoyageEnd`, `DurationHours`, `TotalDistanceKM` (sum of `SegmentDistanceKM`), `AvgSpeed`, `AvgLAT`, `AvgLON`.
- Partitioning: voyage summary is partitioned by `VoyageStartDate`. Trajectory data is partitioned by padded `year=YYYY/month=MM/day=DD`.

## What was wrong before
- Every run read the entire `trajectory_points/` dataset (no window pruning).
- Output was written as a single file (`coalesce(1)`), creating a write bottleneck.
- No voyage-level state existed, so no incrementality; each run recomputed everything.

## What we do now
- Read only the requested window’s `trajectory_points` partitions (padded paths).
- Aggregate window rows to per-voyage deltas (min/max time, distance sum, SOG sum/count, LAT/LON sums, point count).
- Merge deltas into a voyage state snapshot (running aggregates) and write it to `voyage_state_by_date=YYYY-MM-DD/` (window end date).
- Upsert `voyage_summary/` by `VoyageStartDate` with dynamic partition overwrite; no single-file write.

## Voyage state schema and storage
- Fields: `MMSI`, `VoyageID`, `VoyageStart`, `LastTime`, `TotalDistanceKM`, `SumSOG`, `CountSOG`, `SumLAT`, `SumLON`, `PointCount`, `is_complete`.
- Location: dated snapshots only under `voyage_state_by_date=YYYY-MM-DD/` (no separate “latest” table).
- Seed for a window run: prior-day snapshot (`window_start - 1 day`).

## Window execution flow
1) Fact 1 writes padded `trajectory_points` partitions for the window.
2) Fact 2:
   - Reads only those trajectory partitions.
   - Finds voyages present in the window and computes window deltas.
   - Reads prior-day voyage state snapshot and merges deltas into running totals (`VoyageStart` preserved; `LastTime` updated).
   - Applies the completion rule (below).
   - Writes the updated state snapshot to `voyage_state_by_date=window_end/`.
   - Writes/updates `voyage_summary/` partitions for the `VoyageStartDate` values touched in this run.

## Completion rule
- If a voyage appears in the current window: `is_complete = false`.
- If a voyage does not appear: mark complete when `LastTime < window_end - 24h`. If data arrives later for the same `VoyageID`, it reopens (`is_complete = false`) and continues accumulating.

## Validation checklist per window
- Padded trajectory partitions exist for the window dates.
- `voyage_state_by_date=window_end/` is written.
- `voyage_summary/` partitions for the touched `VoyageStartDate` values are updated.



1. Spot-check metrics on a partition (e.g., 2024-01-02):

SELECT
  count(*) AS voyages,
  sum(totaldistancekm) AS sum_dist_km,
  avg(avgspeed) AS avg_speed,
  sum(pointcount) AS sum_points
FROM voyage_summary
WHERE voyagestartdate = DATE '2024-01-02';

#	voyages	sum_dist_km	avg_speed	sum_points
1	5047	201226.6658735621	7.930135567784699	102413

2.Row counts by partition:

SELECT voyagestartdate, count(*) AS voyages
FROM voyage_summary
GROUP BY voyagestartdate
ORDER BY voyagestartdate;

#	voyagestartdate	voyages
1	2024-01-01	5047
2	2024-01-02	5047
3	2024-01-03	5047
4	2024-01-04	5047


3. Consistency vs trajectory for the same dates

SELECT COUNT(*) AS voyages_in_traj
FROM (
    SELECT DISTINCT mmsi, voyageid
    FROM trajectory_points
    WHERE (year, month, day) BETWEEN (2024, 1, 1) AND (2024, 1, 3)
) AS t;


#	voyages_in_traj
1	26566


-- distinct voyages in summary for the same start dates
SELECT count(*) AS voyages_in_summary
FROM voyage_summary
WHERE voyagestartdate BETWEEN DATE '2024-01-01' AND DATE '2024-01-03';

#	voyages_in_summary
1	15141



WITH traj_starts AS (
  SELECT mmsi, voyageid, date(min(BaseDateTime)) AS start_date
  FROM trajectory_points
  WHERE (year, month, day) BETWEEN (2024,1,1) AND (2024,1,4)
  GROUP BY mmsi, voyageid
)
-- counts by start_date from trajectory
SELECT start_date, COUNT(*) AS voyages FROM traj_starts GROUP BY start_date ORDER BY start_date;

#	start_date	voyages
1	2024-01-01	16282
2	2024-01-02	5306
3	2024-01-03	4978
4	2024-01-04	5047


-- compare to summary
SELECT VoyageStartDate, COUNT(*) AS voyages
FROM voyage_summary
GROUP BY VoyageStartDate
ORDER BY VoyageStartDate;

#	VoyageStartDate	voyages
1	2024-01-01	10028
2	2024-01-02	1684
3	2024-01-03	4978
4	2024-01-04	5047


-- find any voyages missing from summary
SELECT COUNT(*) AS missing
FROM traj_starts t
LEFT JOIN voyage_summary s
  ON t.mmsi = s.mmsi AND t.voyageid = s.voyageid
WHERE s.mmsi IS NULL;

#	missing
1	26566
