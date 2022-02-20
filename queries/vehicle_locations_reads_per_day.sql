/*Get the daily number of vehicles seen and the number of sensor reads taken over the last month.*/

SELECT DATE(read_time)                                 AS date,
       DATE_FORMAT(read_time, "%a")                    AS weekday,
       COUNT(DISTINCT id)                              AS vehicles_seen,
       COUNT(1)                                        AS reads_taken,
       SUM(COUNT(1)) OVER (ORDER BY DATE(read_time))   AS cumul_reads

  FROM TTC.vehicle_locations
 WHERE DATE(read_time) >= SUBDATE(CURRENT_DATE(), INTERVAL 1 MONTH)
 GROUP BY 1,2
 ORDER BY 1,2;