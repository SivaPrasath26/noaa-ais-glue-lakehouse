# AIS Trajectory Sampling and Downsampling Report

This document provides a detailed technical explanation of the AIS trajectory sampling strategy, the behavior observed in January 2024 test data, and the rationale behind the chosen sampling parameters. It focuses exclusively on **sampling**, **trajectory thinning**, and **downstream analytics readiness**. All incremental/stateful explanations have been removed.

---

# 1. Overview

AIS datasets produce extremely dense temporal position records. Even for anchored vessels, AIS transmits constant updates, resulting in thousands of redundant points per day. For dashboards, geospatial APIs, analytics layers, and interactive UIs, retaining full‑resolution AIS data is inefficient.

A controlled downsampling algorithm is required to:

* preserve movement geometry
* preserve day boundaries and voyage transitions
* minimize disk footprint and downstream scan cost
* eliminate redundant positions (anchored drift, repeated lat/lon)
* maintain accurate segment distances after thinning

This report details the sampling logic, evaluates observed behavior, and justifies the final parameter selection.

---

# 2. Characteristics of AIS Data Relevant to Sampling

AIS signals have the following characteristics:

* **Anchored vessels** may transmit thousands of identical or near-identical positions per day.
* **Low‑speed movement (0.1–5 kn)** often reflects drift or station‑keeping, not true navigational movement.
* **Meaningful spatial transitions** usually occur:

  * when a vessel departs/arrives
  * during maneuvering
  * during sustained transit at >10 kn
* **AIS temporal density is uneven**, ranging from 2s to 2min intervals across the globe.

Sampling must respect these characteristics.

---

# 3. January 2024 Behavioral Analysis

Evaluated MMSI: **367515090** on **2024‑01‑02**

## 3.1 Staging Summary

* Total rows: **1,223**
* Rows with SOG > 0.5 kn: **41**
* Rows with movementflag = 1: **95**
* Anchored/stationary rows: **~1,128**

Most points show negligible spatial variance; lat/lon differ only in the 4th–5th decimal, consistent with drift.

## 3.2 Movement Window

A short, low-speed maneuver occurred between **19:46 and 20:16 UTC**, with SOG peaks near 4–5 kn.

Outside that period, the vessel exhibited anchored drift.

## 3.3 Behavior of the Aggressive Sampler

The aggressive sampler retained:

* **25 total points** for the day
* **1 moving point** (entire 20–30 min cluster compressed into one point)

Reason: All low-speed movement fell into a single 30‑minute bucket, so only one point was kept.

## 3.4 Impact

* For BI metrics: acceptable
* For dashboard path visualization: undesirable; short movements collapse to a single hop

---

# 4. Revised Sampling Strategy

A refined sampling algorithm was selected to balance fidelity and row count.

## 4.1 Core Principles

1. **Fast movement (>10 kn)** should sample frequently (10 min).
2. **Slow movement (<10 kn)** should sample moderately (15 min).
3. **Anchored drift** should sample per day.
4. **Each moving bucket should retain both first and last points** to preserve geometry.
5. **Daily first and last points** are always retained.
6. **Voyage boundaries** are preserved.

---

# 5. Final Recommended Parameters

```python
fast_sog_threshold_knots = 10.0
fast_interval_min = 10
slow_interval_min = 15
anchor_interval_min = 60
```

### Rationale

* **10 kn threshold** separates maneuvering from transit.
* **10 min fast interval** preserves transit curvature.
* **15 min slow interval** captures low‑speed navigational adjustments.
* **60 min anchored interval** collapses drift-heavy periods.

These values provide a stable compromise between accuracy and file size.

---

# 6. Final Sampling Logic (Pseudo‑Code)

```
for each record:
    classify movement_state:
        anchored if SOG < 0.5
        slow_move if 0.5 <= SOG < 10
        fast_move if SOG >= 10

assign buckets:
    bucket_fast  = timestamp // (fast_interval_min  * 60)
    bucket_slow  = timestamp // (slow_interval_min  * 60)
    bucket_anchor = timestamp // (anchor_interval_min * 60)

within each bucket:
    keep first and last moving point (fast or slow)
    keep first and last anchored point

always keep:
    first point of day
    last point of day
    all voyage boundary points

recompute segment distances using haversine on the thinned timeline
```

---

# 7. Expected Row Counts After Sampling

For full U.S. coverage (15k–25k MMSI/day):

* Typical curated sampled rows per day: **300k–450k**
* Drift-heavy ports remain compressed
* High-speed ocean transits remain detailed

This scale is ideal for:

* Athena queries
* GeoJSON API assembly
* dashboard rendering
* compact Parquet storage (tens of MB per day instead of hundreds)

---

# 8. Why This Sampling Is Suitable for Downstream Consumption

* Minimizes storage and compute cost
* Preserves voyage geometry
* Removes AIS noise (anchored drift, micro-jitter)
* Ensures clean map visualizations
* Retains temporal correctness and distance accuracy
* Simplifies downstream tools (FastAPI/Flask/BI layers) by serving small, clean trajectory subsets

---

# 9. Summary

This sampling strategy:

* captures all meaningful vessel movement paths
* compresses non-movement periods
* maintains navigational fidelity where required
* stays computationally lightweight
* produces curated datasets optimal for dashboards, analytics, and APIs

This document now fully reflects the sampling logic without any incremental/stateful processing content.

---

# 10. AIS Sampling Deep‑Dive

## 10.1 Why Anchored Data Dominates AIS Volumes

AIS transmissions are periodic regardless of vessel movement. When anchored, vessels may still broadcast:

* every few seconds (Class A)
* every 30 seconds (Class B)
* even denser in congested waterways

This produces thousands of nearly identical lat/lon points daily. Without sampling, a single anchored vessel can generate more points than all its moving periods combined. Thus, anchor-aware thinning is mandatory for any production‑grade AIS warehouse.

### Key anchored behaviors:

* Jitter of 1–8 meters due to GPS noise
* Micro-drift from tides or currents
* Static reporting (same coordinates with slight variance)

Sampling must aggressively collapse these patterns while preserving daily position checkpoints.

---

## 10.2 Spatial Fidelity Requirements for Movement

Movement requires more careful treatment:

* Sub‑15‑minute movements can represent turning basins, harbor maneuvers, or channel transits.
* A single point per bucket is insufficient; losing the exit/entry direction breaks the path.

Therefore:

* **First + Last per bucket** is mandatory to preserve segment geometry.
* Bucket sizes must reflect realistic vessel speeds (fast vs slow SOG thresholds).

---

## 10.3 Why 10 kn Is a Reliable Fast‑Movement Threshold

Commercial vessels typically operate:

* < 5 kn: maneuvering, docking, station‑keeping
* 5–12 kn: harbor departure/approach, inland waterways
* > 12 kn: transit or coastal navigation

Setting the fast threshold at **10 kn** ensures:

* High‑resolution sampling for actual voyages
* Reduced resolution for low‑speed, low‑value points

---

## 10.4 Anchored Interval Justification (24 hours / daily endpoints)

Daily endpoints for anchored vessels provide:

* Safe temporal anchoring of each vessel
* A minimal, predictable baseline dataset
* No overrepresentation of static vessels

For dashboard use‑cases, intermediate anchored positions do not add interpretive value unless the vessel performs drifts > 500–1000 meters, which is rare.

---

## 10.5 Required Columns and Assumptions

Sampling assumes the input DataFrame contains:

* **BaseDateTime** (timestamp)
* **LAT, LON** (double)
* **SOG** (knots)
* **movement_state** (derived: "anchored" or "moving")
* **MMSI**
* **VoyageID** (from upstream segmentation)

These columns must already be cleaned (valid SOG, valid coordinates, correct ordering).

---

## 10.6 How Daily First/Last Are Guaranteed

The sampler applies:

```
w_day_first  = row_number over (MMSI, year, month, day ordered asc)
w_day_last   = row_number over (MMSI, year, month, day ordered desc)
```

This guarantees:

* Exactly one earliest point per vessel per day
* Exactly one latest point per vessel per day

These two rows are kept regardless of movement.

This ensures:

* Daily anchor point for dashboards
* Daily bounding timestamps for voyage resumes
* Zero dependency on presence of 00:00:00 or 23:59:59 timestamps

---

## 10.7 Bucket Design and Temporal Quantization

Movement buckets use:

```
bucket_move = floor(unix_timestamp / bucket_seconds)
```

Advantages:

* Deterministic partitioning
* Homogeneous bucket widths
* Time-range sampling independent of AIS send rate

This makes the sampler robust to:

* bursty AIS patterns
* gaps due to reception failures
* heterogeneous Class A/B reporting rates

---

## 10.8 Post-Sampling Distance Recalculation

After thinning, segment distances are recomputed using:

```
PrevLAT_thin = lag(LAT)
PrevLON_thin = lag(LON)
SegmentDistanceKM = haversine(Prev, Curr)
```

This ensures:

* accurate spatial distances on the **thinned** timeline
* correct aggregation in voyage-level summaries
* preserved analytics correctness (total distance, average SOG)

Without this step, downstream distances would be inconsistent with the reduced point-set.

---

## 10.9 Expected Output Distribution

For typical U.S. coastal AIS patterns:

* 40–60 percent of vessels are anchored at any given time
* Movement usually happens in 10–20 percent of day timeline

Thus the sampled dataset typically becomes:

* 80–90 percent reduced size compared to staging
* retaining all meaningful segments

---

# 11. End-to-End Interpretation

This sampling model:

* compresses static periods aggressively
* preserves dynamic periods with controlled fidelity
* guarantees temporal correctness
* ensures deterministic output independent of AIS noise

It forms a **clean, lightweight, trajectory‑optimized dataset** for any downstream consumer such as:

* dashboards
* route reconstruction
* anomaly detection
* transit prediction
* geospatial web APIs

All sections in this expanded document now provide deeper clarification while preserving all original content.
