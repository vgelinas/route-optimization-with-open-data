/*Data preparation method: Load location and direction data for all stops.
This is used for predicting vehicle times at each stops, joining it to the trips table.*/

SELECT directions.tag               AS direction_tag,
	   stops.tag                    AS stop_tag,
	   stops.lat                    AS lat,
	   stops.lon                    AS lon,
	   stops.stop_along_direction   AS stop_order
  FROM TTC.directions directions
  LEFT JOIN TTC.stops stops ON stops.direction_tag = directions.tag
  ORDER BY direction_tag, stop_order