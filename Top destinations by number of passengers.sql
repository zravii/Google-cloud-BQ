SELECT DEST_CITY_NAME, ROUND(SUM(CAST(PASSENGERS AS INT64))) AS total_passengers
FROM sp24-i535-ravishar-flightdata.dataset_flight.clean_data
GROUP BY DEST_CITY_NAME
ORDER BY total_passengers DESC
LIMIT 10;

