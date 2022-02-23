/*Identify full trips along a direction for each vehicle in TTC.vehicle_locations.

Approach: TTC.vehicle_locations contains minute-by-minute location samples for each vehicle.
Calculating times to previous/next samples seen on the same direction, the trip boundaries 
correspond to large jumps in these time values. We use a specified threshold to pick out
these jumps, currently at 15min. 

Once the boundaries of trips are identified, we join a trip number to the sample table at their 
read_time value, and then backfill the intermediate samples's null values to that same trip number.
(Note: From the way we are filling trip numbers from null values, it's enough to identify trip starts 
only because a trip's end is right before another trip's start. This is what we do below.)
*/

SET @num_days = 2;  -- consider num_days's worth of data including today
SET @threshold = 900;  -- 15min in seconds

WITH base AS (
    SELECT id AS vehicle_id,
           direction_tag,
           read_time, 
           TIMESTAMPDIFF(SECOND, LAG(read_time) OVER (PARTITION BY id, direction_tag ORDER BY read_time), read_time) AS sec_to_prev
           
      FROM TTC.vehicle_locations 
     WHERE direction_tag <> 'None' -- TODO: Fix null insertion issue in sqlalchemy ORM, then change this part of the query. 
       AND DATE(read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL (@num_days - 1) DAY)
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
  FROM TTC.vehicle_locations loc
  LEFT JOIN trips ON loc.id=trips.vehicle_id AND loc.direction_tag=trips.direction_tag AND loc.read_time=trips.read_time
 WHERE DATE(loc.read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL (@num_days - 1) DAY)
 ORDER BY loc.id, loc.read_time;