# Data-Lake-with-AWS-EMR-and-PySpark
Data lake project submission for Udacity Data Engineer for AI Applications Nanodegree

## Introduction
This project aims to extract songs metadata and user activity data from JSON  files residing in an AWS S3 bucket using aws EMR and PySpark, build a star schema with the data and save them in parquet files back into an AWS S3 bucket. 

## Project Dataset
The following two datasets used in this project are two subsets of real data from the [Million Song Dataset](http://millionsongdataset.com/) and of generated data by this [event simulator](https://github.com/Interana/eventsim).

AWS S3 links for each:
-   Song data: `s3://udacity-dend/song_data`
-   Log data: `s3://udacity-dend/log_data`

## Project files
- etl.py: loads data from S3, creates star schema data frames and save them into an AWS S3 bucket
- data-lake-etl.ipynb : step by step extracting the data and writing them back to AWS S3.
- dl.cfg: configration file for the AWS secret keys

## Project steps
- Using PySpark and AWS EMR, the data is extracted from JSON files from an S3 bucket. 
- The data is selected and saven into spark dataframes.
- The dataframes are stored in parquet files in an S3 bucket

## Star schema tables\dataframes
![enter image description here](https://github.com/BaZom/Data-warehouse-with-AWS-S3-and-Redshift/blob/848476c6f991f098374eba1e0247dcb8d3350468/star_schema.png)

## How to run
- Create an EMR cluster
- Fill in the dl.cfg file with the secret keys
- Creat an S3 bucket for the parquet files
- run etl.py
- data-lake-etl.ipynb can also be used for testing
