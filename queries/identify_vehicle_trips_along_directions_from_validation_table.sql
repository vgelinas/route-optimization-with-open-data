/*Identify full trips along a direction for each vehicle in TTC.vehicle_locations_validation.

This is the same as the other query, except this table has granular samples for specific 
'validation' vehicles to serve as benchmark. We also pull the entire table, since it's
capped to a manageable size.

We also reduce the minimal threshold for identifying trip boundaries to 5min,
which is more accurate with such frequent samples. 
*/

SET @threshold = 300;  -- 5min in seconds

WITH base AS (
    SELECT id AS vehicle_id,
           direction_tag,
           read_time, 
           TIMESTAMPDIFF(SECOND, LAG(read_time) OVER (PARTITION BY id, direction_tag ORDER BY read_time), read_time) AS sec_to_prev
      FROM TTC.vehicle_locations_validation 
     WHERE direction_tag <> 'None' -- TODO: Fix null insertion issue in sqlalchemy ORM, then change this part of the query. 
     ORDER BY id, direction_tag, read_time
),
     start_times AS (
    SELECT vehicle_id, 
           direction_tag,
           read_time,
           ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY read_time) AS trip_number
      FROM base 
     WHERE sec_to_prev IS NULL OR sec_to_prev >= @threshold
),
     base_with_starts AS (
    SELECT loc.vehicle_id, loc.direction_tag, loc.read_time, loc.sec_to_prev, 
           starts.trip_number
      FROM base loc
      LEFT JOIN start_times starts ON loc.vehicle_id=starts.vehicle_id AND loc.direction_tag=starts.direction_tag AND loc.read_time=starts.read_time
     ORDER BY loc.vehicle_id, loc.read_time
),
     base_fill_helper AS (
    SELECT *, SUM(CASE WHEN trip_number IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY vehicle_id ORDER BY read_time) AS row_group
      FROM base_with_starts
),
     trips AS (
    SELECT vehicle_id, direction_tag, read_time, 
           FIRST_VALUE(trip_number) OVER (PARTITION BY row_group ORDER BY read_time) AS trip_number
      FROM base_fill_helper
     ORDER BY vehicle_id, read_time
)

SELECT loc.id as vehicle_id, loc.direction_tag, loc.lat, loc.lon, loc.read_time, trips.trip_number
  FROM TTC.vehicle_locations_validation loc
  LEFT JOIN trips ON loc.id=trips.vehicle_id AND loc.direction_tag=trips.direction_tag AND loc.read_time=trips.read_time
 ORDER BY loc.id, loc.read_time;