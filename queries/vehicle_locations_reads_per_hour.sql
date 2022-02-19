/*Get the hourly number of vehicles seen and the number of sensor reads taken today.*/

SELECT DATE(read_time)                                 AS date,
	  HOUR(read_time)                                 AS hour,
       COUNT(DISTINCT id)                              AS vehicles_seen,
       COUNT(1)                                        AS reads_taken,
       SUM(COUNT(1)) OVER (ORDER BY HOUR(read_time))   AS cumul_reads

  FROM TTC.vehicle_locations
 WHERE DATE(read_time) = CURRENT_DATE()
 GROUP BY 1,2
 ORDER BY 2;