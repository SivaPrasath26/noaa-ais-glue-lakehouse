-- Total rows per day
SELECT year, month, day, COUNT(*) AS rows
FROM trajectory_points
GROUP BY year, month, day
ORDER BY year, month, day;


#	year	month	day	rows
1	2024	1	1	361320
2	2024	1	2	365921

-- Distinct MMSI per day
SELECT year, month, day, COUNT(DISTINCT mmsi) AS mmsi_cnt
FROM trajectory_points
GROUP BY year, month, day
ORDER BY year, month, day;

#	year	month	day	mmsi_cnt
1	2024	1	1	14868
2	2024	1	2	15130

-- All rows for one MMSI/day
SELECT basedatetime, lat, lon, sog, movement_state
FROM trajectory_points
WHERE mmsi = 367515090 AND year = 2024 AND month = 1 AND day = 2
ORDER BY basedatetime;

#	basedatetime	lat	lon	sog	movement_state
1	2024-01-02 00:00:08.000	39.21166	-76.58143	0.0	anchored
2	2024-01-02 01:59:37.000	39.21167	-76.5814	0.0	anchored
3	2024-01-02 02:00:07.000	39.21167	-76.5814	0.0	anchored
4	2024-01-02 03:59:48.000	39.21167	-76.58141	0.0	anchored
5	2024-01-02 04:00:08.000	39.21166	-76.58141	0.0	anchored
6	2024-01-02 05:58:59.000	39.21164	-76.58141	0.0	anchored
7	2024-01-02 06:02:07.000	39.21164	-76.58141	0.0	anchored
8	2024-01-02 07:59:08.000	39.21166	-76.58141	0.0	anchored
9	2024-01-02 08:00:08.000	39.21166	-76.58145	0.0	anchored
10	2024-01-02 09:59:58.000	39.21166	-76.58138	0.0	anchored
11	2024-01-02 10:00:08.000	39.21165	-76.58139	0.0	anchored
12	2024-01-02 11:59:28.000	39.21165	-76.58144	0.0	anchored
13	2024-01-02 12:00:07.000	39.21166	-76.58143	0.0	anchored
14	2024-01-02 13:00:18.000	39.21084	-76.57946	3.7	moving
15	2024-01-02 13:59:38.000	39.21215	-76.58091	0.0	anchored
16	2024-01-02 14:00:08.000	39.21214	-76.58093	0.0	anchored
17	2024-01-02 15:59:59.000	39.21166	-76.58102	0.0	anchored
18	2024-01-02 16:00:18.000	39.21167	-76.58101	0.0	anchored
19	2024-01-02 17:59:48.000	39.21168	-76.58097	0.0	anchored
20	2024-01-02 18:00:09.000	39.21171	-76.58097	0.0	anchored
21	2024-01-02 19:58:58.000	39.21193	-76.58002	0.2	anchored
22	2024-01-02 20:02:39.000	39.21208	-76.58018	0.2	anchored
23	2024-01-02 21:59:37.000	39.21168	-76.58141	0.0	anchored
24	2024-01-02 22:00:08.000	39.21169	-76.5814	0.0	anchored
25	2024-01-02 23:59:08.000	39.21171	-76.58147	0.0	anchored

-- Movement state distribution for a MMSI/day
SELECT movement_state, COUNT(*) AS rows
FROM trajectory_points
WHERE mmsi = 367515090 AND year = 2024 AND month = 1 AND day = 2
GROUP BY movement_state;

#	movement_state	rows
1	moving	1
2	anchored	24


-- Min/max timestamps per day
SELECT year, month, day,
       MIN(basedatetime) AS first_ts,
       MAX(basedatetime) AS last_ts
FROM trajectory_points
GROUP BY year, month, day
ORDER BY year, month, day;

#	year	month	day	first_ts	last_ts
1	2024	1	1	2024-01-01 00:00:00.000	2024-01-01 23:59:59.000
2	2024	1	2	2024-01-02 00:00:00.000	2024-01-02 23:59:59.000

-- Count distinct voyages per day
SELECT year, month, day, COUNT(DISTINCT voyageid) AS voyages
FROM trajectory_points
GROUP BY year, month, day
ORDER BY year, month, day;

#	year	month	day	voyages
1	2024	1	1	5
2	2024	1	2	8
