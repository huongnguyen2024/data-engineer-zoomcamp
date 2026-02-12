## Module 3: Data Warehouse and BigQuery

#### Definitions of Data Warehouse and BigQuery
• OLTP (Online Transaction Processing) runs in the backend, real-time, for small and updated data tasks. \
• OLAP (Online Analytics Processing) runs as needed for analytics and data science purposes, for large data tasks with comparison.  \
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

## HW 3: Data Warehousing
#### Question 1



