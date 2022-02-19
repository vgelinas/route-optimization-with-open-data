/*Measure the route activity on different direction tags based on weekday. 
This helps understand if unusual direction tags belong to special time periods,
and how frequently they are used.

Hypothesis: some of these tags seem to be either
- morning utility runs (maybe for testing the tracks?)
- helper runs (to handle overflow?)

The first type occurs once or twice early morning each day, 
while the second type seems to kick in at rush hour.
*/
SET @route_tag = '501';
SET @num_weeks_back = 1;

WITH route_readings AS ( 
SELECT routes.tag AS route_tag,
	  dir.tag AS direction_tag,
       dir.title AS direction_title,
       dir.name AS heading,
       loc.id AS vehicle_id,
       loc.read_time
  FROM TTC.routes routes
  LEFT JOIN TTC.directions dir ON routes.tag=dir.route_tag
  LEFT JOIN TTC.vehicle_locations loc ON dir.tag=loc.direction_tag
 WHERE routes.tag=@route_tag
)

SELECT route_tag, 
	  direction_tag, 
       direction_title,
       heading,
       DATE_FORMAT(DATE(read_time), "%w") AS weekday,
       DATE_FORMAT(DATE(read_time), "%a") AS day,
	   COUNT(distinct vehicle_id) AS vehicles_seen,
       COUNT(1) AS reads_taken,
       DATE_FORMAT(MIN(read_time), "%T") AS min_read_time,
       DATE_FORMAT(MAX(read_time), "%T") AS max_read_time
  FROM route_readings
 WHERE DATE(read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL @num_weeks_back WEEK)
 GROUP BY 1,2,3,4,5,6
 ORDER BY 1,4,2,5;
