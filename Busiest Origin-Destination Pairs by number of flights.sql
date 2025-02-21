SELECT ORIGIN_CITY_NAME, DEST_CITY_NAME, COUNT(*) AS num_flights
FROM sp24-i535-ravishar-flightdata.dataset_flight.clean_data
GROUP BY ORIGIN_CITY_NAME, DEST_CITY_NAME
ORDER BY num_flights DESC
LIMIT 10;
