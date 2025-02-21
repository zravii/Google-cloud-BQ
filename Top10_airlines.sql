SELECT UNIQUE_CARRIER_NAME, COUNT(*) AS total_flights
FROM sp24-i535-ravishar-flightdata.dataset_flight.clean_data
GROUP BY UNIQUE_CARRIER_NAME
ORDER BY total_flights DESC;
