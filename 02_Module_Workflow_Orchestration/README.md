## HW 2: Workflow Orchestration using Kestra

#### Question 1
By adding this command for the extract task: 
```bash 
- ls -lh {{render(vars.file)}} 
```
This Linux command list the extracted file which is yellow_tripdata_2020-12.csv after it has been unzipped. \
When the execution is done -> Logs -> extract -> -rw-r--r-- 1 root root 129M Feb  2 18:52 yellow_tripdata_2020-12.csv  

#### Question 2 
When the variable file has been rendered, it appears as green_tripdata_2020-04.csv 

#### Question 3 Using BigQuery
```sql
SELECT COUNT(*) 
FROM zoomcamp.yellow_tripdata  
WHERE filename LIKE "%_2020_%"; 
```
#### Question 4 Using BigQuery
```sql
SELECT COUNT(*) 
FROM zoomcamp.green_tripdata 
WHERE filename LIKE "%_2020_%";
```
#### Question 5 Using BigQuery
```sql
SELECT COUNT(*) 
FROM zoomcamp.yellow_tripdata  
WHERE filename LIKE "%_2021_03%"; 
```
#### Question 6
Plugin trigger.Schedule has a property named timezone, and we can find the desired timezone to trigger the flow using Wikipedia table and set timezone as following: 
```yaml
triggers: 
  - id: green_schedule 
    type: io.kestra.plugin.core.trigger.Schedule 
    cron: "0 9 1 * *"
    timezone: America/New_York
    inputs:
      taxi: green
```
