/*Preparation method: Segment vehicle location data into trips.

The TTC.vehicle_locations table consists of minute-by-minute vehicle
location data, and we are interested in the time of visit of vehicles
at each stop to feed into various transit statistics calculations. 

This query processes the minute data into segments, each representing 
a trip along one direction. A single trip consists of samples of 
timestamped location data (latitude, longitude), which can be used
to infer the time-of-visit at stops on this direction by feeding 
the stop's location data as input and performing regression. 

The query is parameterized. Trips are partitioned by last timestamp,
and we fetch all trips which end in the interval [%(left)s, %(right)s).  
For example, a trip lasting from 15:30 to 16:03 will be fetched if we 
query from 16:00 to 17:00, since 16:03 is between 16:00-17:00. 

As a consequence, this query produces the same result whether we 
apply it to an entire time interval or split-and-combine the results. 
E.g. querying for 16:00-17:00 and 17:00-18:00 produces the same trips
as querying for 16:00-18:00. 

NOTE: We fetch slightly more than the [left, right) interval in order
to properly identify trip boundaries. See 'offset' arg below. 

-----------
Args: 
    - left:                Lower bound for trip's last timestamp. Must have
                        timestamp >= left for the trip to be considered. 
    - right:             Upper bound for trip's last timestamp. Must have
                        timestamp < right for the trip to be considered. 
    - offset:             Number of hours removed from 'left' and added to 
                        'right' when fetching timestamps to create trips.
                        E.g. when querying for 12:00-18:00 with offset=3,
                        we read all timestamps between 9:00-21:00. This 
                        insures that trip boundaries are identified 
                        properly, i.e. no trip is cut short (missing
                        the part before 12:00), or improperly identified 
                        as ending before 18:00 if it is still going. 
                        
                        Offset value should be above the max length of 
                        a trip on the network (typically offset=3 is good).

-----------
Return columns:
    - vehicle_id:        unique vehicle identifier
    - direction_tag:     unique direction identifier
    - lat:                latitude
    - lon:                longitude
    - read_time:        vehicle reading timestamp
    - trip_id:            trip unique identifier (timestamp at trip start)

*/

WITH preprocessed_base AS (        /*Pull data for the wanted period (with offset), and fix isolated sensor errors in the direction_tag.*/
    SELECT id AS vehicle_id,
           CASE WHEN LAG(direction_tag) OVER (PARTITION BY id ORDER BY read_time) = LEAD(direction_tag) OVER (PARTITION BY id ORDER BY read_time) 
                THEN LAG(direction_tag) OVER (PARTITION BY id ORDER BY read_time)  
                ELSE direction_tag 
            END AS direction_tag,  /*Fixes random missing direction_tag in the middle of a trip, or places where the tag randomly flips (e.g. from east to west).*/
           lat, 
           lon, 
           read_time
           
      FROM TTC.vehicle_locations
     WHERE read_time >= DATE_SUB(%(left)s, INTERVAL %(offset)s HOUR)
       AND read_time < DATE_ADD(%(right)s, INTERVAL %(offset)s HOUR)
     ORDER BY vehicle_id, read_time
),
     time_to_prev_ts AS (        /*For each vehicle, calculate time to previous timestamp seen on the same direction.*/
    SELECT vehicle_id,
           direction_tag,
           read_time, 
           TIMESTAMPDIFF(SECOND, 
                LAG(read_time) OVER (PARTITION BY vehicle_id, direction_tag ORDER BY read_time),
                read_time
                ) AS sec_to_prev_ts
                
      FROM preprocessed_base
     WHERE direction_tag <> 'None'  -- TODO: Fix null insertion issues in ORM, then change this part of the query. 
),
     start_times AS (            /*Identify trip segments by inspecting the sec_to_prev_ts field for large deviations.*/
    SELECT vehicle_id, 
           direction_tag,
           read_time,
           ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY read_time) AS trip_number
           
      FROM time_to_prev_ts
     WHERE sec_to_prev_ts IS NULL OR sec_to_prev_ts >= 600  -- 10min in seconds, used as threshold
),
     loc_data_with_starts AS (     /*Left join start_times to the location data, tagging all trip starts with a trip_id.*/
    SELECT base.*, 
           start_times.trip_number
           
      FROM preprocessed_base base 
      LEFT JOIN start_times ON base.vehicle_id=start_times.vehicle_id 
                            AND base.direction_tag=start_times.direction_tag 
                            AND base.read_time=start_times.read_time
),
     loc_data_fill_helper AS (     /*Create a row_group column, identifying which rows belong to the same trip.*/
    SELECT *,
           SUM(CASE WHEN trip_number IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY vehicle_id ORDER BY read_time) AS row_group
           
      FROM loc_data_with_starts
),
     loc_data_with_trips AS (    /*Use row_group to create trip_start and trip_end time columns, constant over each trip.*/
    SELECT vehicle_id,
           direction_tag,
           lat, 
           lon, 
           read_time, 
           MIN(read_time) OVER (PARTITION BY vehicle_id, row_group) AS trip_start,
           MAX(read_time) OVER (PARTITION BY vehicle_id, row_group) AS trip_end  
           
      FROM loc_data_fill_helper
)


SELECT vehicle_id,
       direction_tag, 
       lat, 
       lon, 
       read_time, 
       trip_start AS trip_id 
       
  FROM loc_data_with_trips
 WHERE trip_end >= %(left)s
   AND trip_end < %(right)s
   AND direction_tag <> 'None'  -- TODO: Fix null insertion issues in ORM, then change this part of the query. 
 ORDER BY vehicle_id, read_time;
