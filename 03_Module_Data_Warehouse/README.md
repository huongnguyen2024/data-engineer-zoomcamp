## Module 3: Data Warehouse and BigQuery

#### Definitions of Data Warehouse and BigQuery
• OLTP (Online Transaction Processing) is optimized for transactional workloads, supporting applications with frequent inserts and updates. \
• OLAP (Online Analytics Processing) is optimized for heavy analytical workloads, designed to run complex queries on large historical datasets, for reporting and business intelligence. \
• Data warehouse could be the storage bucket to store raw files in csv, parquet, json \
• BigQuery works with a dataset that contains structured tables, schemas \
• A bucket (data warehouse) and a dataset are not contained in each other; they are independent. They are combined to become a flow of data. \
\ A bucket and a dataset should be in the same region to reduce cost and job speed. \
• There are three options for their interaction with each other:
1. Data warehouse (Bucket) can be a source -> Load data into -> BigQuery 
2. External table: BigQuery reads/queries data directly from a Bucket source  
3. BigQuery exports the data to a Bucket 

#### BigQuery 
1. Partitioning: splitting the dataset into chunks where each chunk represent a specific group of data e.g. based on date. 
This helps reduce cost when querying. Cost is upfront. One column. 
2. Clustering:  more filtered table with grouping into clusters. Cost is unknown. Multiple columns. \
• We should choose Clustering over Partitioning when: \
 ••• Partitioning results in very small amount of granualities, which might exceed the limits on partition tables (4000) \
 ••• Partitioning results in mutation operations too frequently e.g. inserting data into partition tables every few minutes. \
• Automatic re-clustering is done in the background by BigQuery. The automatic re-clustering helps maintain the sort property of the table when a new data is inserted to the a clustered table, as this inserting causes key ranges overlapping, therefore it weakens the sort property of the table. 
3. Best practices: 
• Cost production 
4. Internals of BigQuery
• Record-oriented  
• Column-oriented: BQ uses this structure. Divides the rows to each column before processing to Colossus storage. \
Jupiter network 1TB per second speed \ 
Divides the data into smaller chunks then propagates them into the leaf nodes. 

## Homework 3: Data Warehousing
#### Q1.
What is count of records for the 2024 Yellow Taxi Data? \
Answer: 20,332,093

#### Q2. 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? \ 
Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

#### Q3. 
Why are the estimated number of Bytes different? \ 
Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

#### Q4. 
How many records have a fare_amount of 0? \
Answer: 8,333

#### Q5. 
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy) \ 
Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

#### Q6. 
 Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? \
 Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

 #### Q7. 
 Where is the data stored in the External Table you created? \
 Answer: GCP Bucket

 #### Q8. 
 It is best practice in Big Query to always cluster your data: \
 Answer: False

 #### Q9.
 Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why? \
It took 0 bytes to be read. \
BigQuery has stored the simple query results and keeps the results on disk. Simple queries include the count(*). Since this is precomputed and stored in the materialized table, BigQuery does not scan/read the table for this query, and BigQuery just retrieves this result.


