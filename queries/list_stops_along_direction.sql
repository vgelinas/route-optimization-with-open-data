/*List all stops in order for a given route direction.*/
SET @direction_tag := '329_0_329';

SELECT routes.tag                   AS route_tag,
       routes.title                 AS route_title,
       directions.tag               AS direction_tag,
       directions.title             AS direction_title,
       directions.name              AS heading,
       stops.tag                    AS stop_tag,
       stops.lat                    AS stop_lat,
       stops.lon                    AS stop_lon,
       stops.title                  AS stop_title,
       stops.stop_along_direction   AS stop_order
  FROM TTC.routes routes
  LEFT JOIN TTC.directions directions ON directions.route_tag = routes.tag
  LEFT JOIN TTC.stops stops ON stops.route_tag = routes.tag AND stops.direction_tag = directions.tag
 WHERE directions.tag = @direction_tag
 ORDER BY stops.stop_along_direction;
       
