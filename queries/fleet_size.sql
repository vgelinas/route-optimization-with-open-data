/*Count the number of vehicles seen up to now.*/

SELECT COUNT(distinct id) AS fleet_size
  FROM TTC.vehicles;