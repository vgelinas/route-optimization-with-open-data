/*List all vehicle locations on a given date.*/
SET @vehicle_id := '1096';
SET @report_date := DATE('2022-02-11');

SELECT loc.id AS vehicle_id,
	   loc.route_tag,
       loc.direction_tag,
       dir.title AS direction_title,
       dir.name AS towards,
       loc.speed_kmhr,
       loc.heading AS heading,
       loc.lat,
       loc.lon,
       DATE_FORMAT(loc.read_time, "%a %d") AS day,
       DATE_FORMAT(loc.read_time, "%T") AS time
  FROM TTC.vehicle_locations loc 
  LEFT JOIN TTC.directions dir ON dir.tag = loc.direction_tag  
 WHERE loc.id = @vehicle_id
   AND DATE(read_time) = @report_date
 ORDER BY read_time;