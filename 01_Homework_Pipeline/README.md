## HW 1: Docker, SQL and Terraform 

#### Question 1 command
```bash
docker run -it --rm --entrypoint=bash python:3.13 \
root@e7ed9a4f7ffb:/# pip --version \
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13) \
root@e7ed9a4f7ffb:/# 
```
#### Question 2 
The hostname and port that pgadmin should use to connect to the postgres database is postgres:5432. 
#### Question 3 SQL query
```sql
SELECT COUNT(*) AS total_trips \
FROM green_taxi_trips \
WHERE trip_distance <= 1 \
            AND CAST(lpep_pickup_datetime AS DATE) >= '2025-11-01' \
		    AND CAST(lpep_pickup_datetime AS DATE) < '2025-12-01' ; 
```
#### Question 4 SQL query
```sql
WITH daily_totals AS ( \
	SELECT SUM(trip_distance) AS total_distance, \
		   CAST(lpep_pickup_datetime AS DATE) AS day \
	FROM green_taxi_trips \
	WHERE trip_distance < 100 \
	GROUP BY day \
	ORDER BY total_distance DESC \
	) \
SELECT * \
FROM daily_totals \
WHERE total_distance = (SELECT MAX(total_distance) FROM daily_totals); 
```
#### Question 5 SQL query
```sql
WITH zones_total AS( \
		SELECT \
			z."Zone", \
			SUM(g.total_amount) AS amount_per_zone \
		FROM green_taxi_trips g \
		JOIN zones z \
		ON g."PULocationID" = z."LocationID" \
		WHERE CAST(g.lpep_pickup_datetime AS DATE) = '2025-11-18' \
		GROUP BY z."Zone" \
		) \
SELECT * \
FROM zones_total \
WHERE amount_per_zone = (SELECT MAX(amount_per_zone) \
						FROM zones_total); 
```
#### Question 6 SQL query
```sql
SELECT z_do."Zone" \
FROM green_taxi_trips g \
JOIN zones z_pu \
	ON g."PULocationID" = z_pu."LocationID" \
JOIN zones z_do \
	ON z_do."LocationID" = g."DOLocationID" \
WHERE z_pu."Zone" = 'East Harlem North' \
	AND g.lpep_pickup_datetime >= '2025-11-01' \
    AND g.lpep_pickup_datetime <  '2025-12-01' \
ORDER BY g.tip_amount DESC \
LIMIT 1;
```
## Note on Dataset 
Parquet files are not committer.
Download example data from: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
