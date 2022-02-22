/*Identify full trips along a direction for each vehicle in TTC.vehicle_locations.

Approach: TTC.vehicle_locations contains minute-by-minute location samples for each vehicle.
Calculating times to previous/next samples seen on the same direction, the trip boundaries 
correspond to large jumps in these time values. To pick these out, we create a table of thresholds 
for each vehicle and direction_tag (basically looking for statistical extreme values).

Once the boundaries of trips are identified, we join a trip number to the sample table at their 
read_time value, and then backfill the intermediate samples's null values to that same trip number.
(Note: From the way we are filling trip numbers from null values, it's enough to identify trip starts 
only because a trip's end is right before another trip's start. This is what we do below.)
*/

SET @from_num_days_ago = 1;  -- pull all samples from yesterday on

WITH base AS (
	SELECT id AS vehicle_id,
		   direction_tag,
		   read_time, 
		   TIMESTAMPDIFF(SECOND, LAG(read_time) OVER (PARTITION BY id, direction_tag ORDER BY read_time), read_time) AS sec_to_prev
           
	  FROM TTC.vehicle_locations 
	 WHERE direction_tag <> 'None' -- TODO: Fix null insertion issue in sqlalchemy ORM, then change this part of the query. 
       AND DATE(read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL @from_num_days_ago DAY)
	 ORDER BY id, direction_tag, read_time
),
	 thresholds AS (
	SELECT vehicle_id,
		   direction_tag,
		   ( AVG(sec_to_prev) + (1/2)*STD(sec_to_prev) ) AS half_sigma  -- Settled upon after some experimentation. Larger is not better, and can lead to "false starts" due to skew. Using a percentile rank would be better here.
	  FROM base
	 WHERE sec_to_prev IS NOT NULL
	 GROUP BY 1,2
),
	 start_times AS (
	SELECT b.vehicle_id, 
		   b.direction_tag,
           b.read_time,
           ROW_NUMBER() OVER (PARTITION BY b.vehicle_id ORDER BY b.read_time) AS trip_number
	  FROM base b
      INNER JOIN thresholds th ON b.vehicle_id=th.vehicle_id AND b.direction_tag=th.direction_tag
	 WHERE b.sec_to_prev IS NULL OR (b.sec_to_prev >= th.half_sigma AND b.sec_to_prev >= 300)  -- detect start times when sec_to_prev is a half-sigma away from the mean, and we enforce a min of 300sec=5min for edge cases for when stdev is nearly 0. 
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
 WHERE DATE(loc.read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL @from_num_days_ago DAY)
 ORDER BY loc.id, loc.read_time;