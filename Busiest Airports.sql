SELECT ORIGIN_CITY_NAME, COUNT(*) AS total_departures
FROM sp24-i535-ravishar-flightdata.dataset_flight.clean_data
GROUP BY ORIGIN_CITY_NAME
ORDER BY total_departures DESC;
