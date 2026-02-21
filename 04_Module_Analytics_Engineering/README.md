## Module 4: Analytics Engineering using dbt
#### analyses: 
- A place for SQL files that you don't want to exposr to the business customers
- Use for data quality reports

#### macros
- Bahave like Python functions (reusable logic)
- New changes: e.g tax rates variables
- Encapsulate logic in one place
- Can be tested

#### models
dbt suggests 3 subfolders:
1. staging
    - Sources (raw table from database)
    - Staging files are 1 to 1 copy of your data with minimal cleaning step such as:
        - Data types
        - Rename columns
2. intermediate
    - Everything that is not raw and not ready to expose to end users
3. marts
    - Where all the final, consumption-ready tables live
    - If it's in marts, it's ready for end users

#### seeds
- A space to upload csv and flat files to add them to dbt later
- Quick and dirty approach (better to fix at source)

#### snapshots
- Take a picture of a table at a moment in time
- Useful to track the history of a column that overwrites itself

#### tests
- A place to put assertions in SQL format
- A place for singular tests
- If this SQL command returns more than 0 rows, the dbt build fails

#### dbt_project.yml
- The most important file in dbt 
- Connection to be created between profiles.yml and a specific project

#### README.md
- Documentation of the project
- Installation/setup guides
- Contact information

    ## Homework 4: Analytics Engineering
 Q1. Given a dbt project with the following structure: 
```  
    models 
    ├── staging 
    │   ├── stg_green_tripdata.sql 
    │   └── stg_yellow_tripdata.sql 
    └── intermediate 
        └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata) 
        If you run dbt run --select int_trips_unioned, what models will be built?     
```
    Answer: int_trips_unioned  
    Because the use of flag select, we will specifically run the model int_trips_unioned only but not the entire model folder. 

Q2. You've configured a generic test like this in your schema.yml. Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data. What happens when you run dbt test --select fct_trips? 
```yaml
    columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```
    Answer: dbt fails the test with non-zero exit code  
    Because the test is not updated to accept the value 6 --> error is detected --> the test fails --> the exit code is non-zero (unsuccessful) 
Q3. Count of records in fct_monthly_zone_revenue? 
```sql
    SELECT COUNT(*)
    FROM taxi_rides_ny.dev.fct_monthly_zone_revenue;
 ```
    Answer: 12,184 records 
Q4. Zone with highest revenue for Green taxis in 2020? 
```sql
    SELECT 
   pickup_zone,
   SUM(revenue_monthly_total_amount) as total_revenue_2020
    FROM taxi_rides_ny.dev.fct_monthly_zone_revenue
    WHERE service_type = 'Green' 
    AND revenue_month >= DATE ('2020-01-01')
    AND revenue_month < DATE ('2021-01-01')
    GROUP BY pickup_zone
    ORDER BY total_revenue_2020 DESC
    LIMIT 1;
```
    Answer: East Harlem North 
Q5.  Total trips for Green taxis in October 2019? 
```sql
    SELECT SUM(total_monthly_trips)
    FROM taxi_rides_ny.dev.fct_monthly_zone_revenue
    WHERE service_type = 'Green' 
    AND revenue_month = DATE ('2019-10-01');
```
    Answer: 384,624 trips 
Q6. Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019. 
    Load the FHV trip data for 2019 into your data warehouse 
    Create a staging model `stg_fhv_tripdata` with these requirements: 
    Filter out records where `dispatching_base_num` IS `NULL `
    Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)  
    What is the count of records in `stg_fhv_tripdata`?  
```sql 
    SELECT COUNT(*)
    FROM taxi_rides_ny.dev.stg_fhv_tripdata;
```
    Answer: 43,244,693 

