/*Prepare the sample vehicle location data from TTC.vehicle_locations for time imputation at each stop location.*/

WITH dir_tag_filter AS (  
    SELECT id AS vehicle_id,
           read_time,
           CASE 
               WHEN LAG(direction_tag) OVER (PARTITION BY id ORDER BY read_time) = LEAD(direction_tag) OVER (PARTITION BY id ORDER BY read_time) 
               THEN LAG(direction_tag) OVER (PARTITION BY id ORDER BY read_time)  
               ELSE direction_tag 
            END AS direction_tag
      FROM TTC.vehicle_locations 
     WHERE DATE(read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL 1 DAY)
     ORDER BY vehicle_id, read_time
),
     base AS (
    SELECT vehicle_id,
           direction_tag,
           read_time, 
           TIMESTAMPDIFF(SECOND, LAG(read_time) OVER (PARTITION BY vehicle_id, direction_tag ORDER BY read_time), read_time) AS sec_to_prev
      FROM dir_tag_filter
     WHERE direction_tag <> 'None' -- TODO: Fix null insertion issue in sqlalchemy ORM, then change this part of the query. 
     ORDER BY vehicle_id, read_time
),
     start_times AS (
    SELECT vehicle_id, 
           direction_tag,
           read_time,
           ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY read_time) AS trip_number
      FROM base 
     WHERE sec_to_prev IS NULL OR sec_to_prev >= 600  -- 10min in seconds
),
     base_with_starts AS (
    SELECT b.vehicle_id, b.direction_tag, b.read_time, b.sec_to_prev, 
           s.trip_number
      FROM base b
      LEFT JOIN start_times s ON b.vehicle_id=s.vehicle_id AND b.direction_tag=s.direction_tag AND b.read_time=s.read_time
     ORDER BY b.vehicle_id, b.read_time
),
     base_fill_helper AS (
    SELECT *, SUM(CASE WHEN trip_number IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY vehicle_id ORDER BY read_time) AS row_group
      FROM base_with_starts
     ORDER BY vehicle_id, read_time
),
     base_with_trips AS (
    SELECT vehicle_id, direction_tag, read_time, 
           FIRST_VALUE(trip_number) OVER (PARTITION BY row_group ORDER BY read_time) AS trip_number
      FROM base_fill_helper
     ORDER BY vehicle_id, read_time
),
     vehicle_locations AS (
    SELECT loc.id as vehicle_id, trips.direction_tag, loc.lat, loc.lon, loc.read_time, trips.trip_number
      FROM TTC.vehicle_locations loc
      LEFT JOIN base_with_trips trips ON loc.id=trips.vehicle_id AND loc.read_time=trips.read_time
     WHERE DATE(loc.read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL 1 DAY) 
     ORDER BY loc.id, loc.read_time
),
     trip_padding AS (
    SELECT vehicle_id, read_time, 
           CASE 
               WHEN LEAD(trip_number) OVER (PARTITION BY vehicle_id ORDER BY read_time) <> trip_number OR trip_number IS NULL
               THEN LEAD(trip_number) OVER (PARTITION BY vehicle_id ORDER BY read_time)
               ELSE trip_number
           END AS trip_number,
           CASE 
               WHEN LEAD(trip_number) OVER (PARTITION BY vehicle_id ORDER BY read_time) <> trip_number OR trip_number IS NULL
               THEN LEAD(direction_tag) OVER (PARTITION BY vehicle_id ORDER BY read_time)
               ELSE direction_tag
           END AS trip_tag 
      FROM vehicle_locations    
     UNION DISTINCT
    SELECT vehicle_id, read_time, 
           CASE 
               WHEN LAG(trip_number) OVER (PARTITION BY vehicle_id ORDER BY read_time) <> trip_number OR trip_number IS NULL
               THEN LAG(trip_number) OVER (PARTITION BY vehicle_id ORDER BY read_time)
               ELSE trip_number
           END AS trip_number,
           CASE 
               WHEN LAG(trip_number) OVER (PARTITION BY vehicle_id ORDER BY read_time) <> trip_number OR trip_number IS NULL
               THEN LAG(direction_tag) OVER (PARTITION BY vehicle_id ORDER BY read_time)
               ELSE direction_tag
           END AS trip_tag 
      FROM vehicle_locations
)

SELECT loc.vehicle_id, pad.trip_tag as direction_tag, loc.lat, loc.lon, loc.read_time, pad.trip_number
  FROM vehicle_locations loc
 INNER JOIN trip_padding pad ON loc.vehicle_id=pad.vehicle_id AND loc.read_time=pad.read_time
 WHERE pad.trip_number IS NOT NULL
 ORDER BY loc.vehicle_id, pad.trip_number, loc.read_time;