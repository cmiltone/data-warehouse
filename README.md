# Project: Cloud Data Warehouse


## Summary
This project aims to extract Sparkify data to a data warehouse on AWS in order to do analytics. It utilizes the AWS Redshift Cluster and AWS S3.
Datasets residing on S3 is loaded onto staging tables using the `copy` command of Postgres. Once loaded the data is then inserted into tables from where analytics can be done.


## Schema

A star schema is utilized for the project. It simplifies denormalization and makes queries simple. Aggregations are also made fast


## Directions
- Setup an AWS Redshift cluster with appropriate role, security group and subnet group
- congire the `dwh.cgf` file making sure to set the ARR, and the cluster details
- install psycopg2 (using pip) and any other necessary libraries
- run:
  - `python3 create_tables.py`
  - `python3 etl.py`

## Analytics
Run the script `analytics.py` to execute queries to fetch the following
- 5 Most played songs
- 5 Hours of the day with the Highest usage