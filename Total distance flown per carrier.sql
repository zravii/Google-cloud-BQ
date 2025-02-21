SELECT UNIQUE_CARRIER_NAME, ROUND(AVG(CAST(PASSENGERS AS FLOAT64))) AS avg_passengers_per_flight
FROM sp24-i535-ravishar-flightdata.dataset_flight.clean_data
GROUP BY UNIQUE_CARRIER_NAME
ORDER BY avg_passengers_per_flight DESC;
