/*Busiest routes by various metrics.*/

WITH routes_readings AS (
SELECT distinct 
	   routes.tag AS route_tag,
       routes.title as route_title,
       loc.direction_tag,
       loc.id AS vehicle_id,
       loc.read_time
  FROM TTC.routes routes
  LEFT JOIN TTC.vehicle_locations loc ON routes.tag=loc.route_tag
)
SELECT route_tag, 
	   route_title,
	   COUNT(DISTINCT vehicle_id) AS vehicles_seen,
       COUNT(1) AS reads_taken,
       COUNT(DISTINCT direction_tag) AS active_directions
  FROM routes_readings
 WHERE DATE(read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL 1 WEEK)
 GROUP BY 1,2
 ORDER BY 4 desc;
