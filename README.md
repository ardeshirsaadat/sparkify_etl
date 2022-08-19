## Project: Data Warehouse
### Introduction
    A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

    As their data engineer, I'm tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.
### Overview

In this project, I build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. 

### Details

The project contains the following scripts:

[create_tables.py](create_tables.py) runs queries to drop and create tables in Redshift [sql_queries.py](sql_queries.py).

[etl.py](etl.py) extracts raw data from S3 into staging tables then loads it into the final tables.

[sql_queries.py](sql_queries.py) contains SQL table creation, copy and insertion commands.


### How to run

To install the libraries needed to run this project, please run:

```commandline
pip install -r requirements.txt
```

The [dwh.cfg](dwh.cfg) file contains the AWS settings needed for each script to connect and run its queries.

```commandline
python create_tables.py
python etl.py
```

