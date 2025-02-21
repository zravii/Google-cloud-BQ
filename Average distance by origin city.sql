SELECT ORIGIN_CITY_NAME,ROUND(AVG(CAST(DISTANCE AS FLOAT64))) AS avg_distance
FROM sp24-i535-ravishar-flightdata.dataset_flight.clean_data
GROUP BY ORIGIN_CITY_NAME
ORDER BY avg_distance DESC;
