/*Assess data quality in TTC.vehicle_locations*/

SELECT COUNT(1)                                                  AS total_rows,
	   SUM(CASE WHEN route_tag = "None" THEN 1 ELSE 0 END)      AS route_tag_NA,
	   SUM(CASE WHEN predictable = 'None' THEN 1 ELSE 0 END)    AS predictable_NA,
	   SUM(CASE WHEN heading = 'None' THEN 1 ELSE 0 END)        AS heading_NA,
       SUM(CASE WHEN speed_kmhr = 'None' THEN 1 ELSE 0 END)      AS speed_kmhr_NA,
       SUM(CASE WHEN lat = 'None' THEN 1 ELSE 0 END)             AS lat_NA,
       SUM(CASE WHEN lon = 'None' THEN 1 ELSE 0 END)             AS lon_NA,
       SUM(CASE WHEN id = 'None' THEN 1 ELSE 0 END)              AS id_NA,
       SUM(CASE WHEN agency_tag = 'None' THEN 1 ELSE 0 END)      AS agency_tag_NA,
       SUM(CASE WHEN read_time IS NULL THEN 1 ELSE 0 END)        AS read_time_NA
  FROM TTC.vehicle_locations;
  
	   