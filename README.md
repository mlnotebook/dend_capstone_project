# Udacity Dataengineering Nanodegree: Capstone Project

This project is the final assignment in the Udacity Nanodegree in Data Engineering.

## US Immigration Data ETL Pipeline

Four datasets are provided and outlined below. The notebook `Capstone_Project.ipynb` contains
the overall data exploration and insights into the data pipeline.
The data ETL pipeline is written as an Airflow Directed Acyclic Graph (DAG) which loads data from S3 into tables
hosted on Amazon Redshift.

## Datasets
### I94 Immigration Data.
* **Source**: US National Tourism and Trade Office [link](https://travel.trade.gov/research/reports/i94/historical/2016.html).
* **Format**: `.sas7bat`
* **Info**: Records of international visitor arrivals by regions and countries, visa type, method of travel, age, state, and port of entry.

### World Temperature Data.
* **Source**: Kaggle [link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). 
* **Format**: `.csv`
* **Info**: Statistics on world temperature by date, city, country, latitude, and longitude.

### U.S. City Demographic Data.
* **Source**: OpenSoft [link](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
* **Format**: `.csv`
* **Info**: Demographics fpr US cities with a population >= 65,000.

### Airport Code Data.
* **Source**: Datahub [link](https://datahub.io/core/airport-codes#data).
* **Format**: `.csv`
* **Info**: A table of IATA (3-letter) or ICAO (4-letter) airport codes and corresponding cities



# Project Writeup

The notebook `Capstone_Project.ipynb` contains the writeup for this project. It contains:
1. Inital data exploration, and  for this project.
2. Method for saving the immigration dataset to `.parquet` format.
3. Inspection of the 4 datasets.
4. Overview of the ETL pipeline.
5. Some insights from the project. 

# Usage 

1. Create `.cfg` file for the project.
2. Run the cells in `Capstone_Project.ipynb` notebook to generate `.parquet` files.
3. Copy data from Udacity workspace to S3
4. Create the Redshift Cluster
5. Database schema

## 1. Create the `.cfg`

This config is used to create the redshift_cluster, as well as provide configuration for the `copy_data_to_s3.py` script. It should be placed in the project root directory.
The ARN under `IAM_ROLE` can be completed after creating the cluster below.

```bash
[CLUSTER]
CLUSTER_TYPE=multi-node
NODE_TYPE=dc2.large
NUM_NODES=2
CLUSTER_IDENTIFIER=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=5439

[IAM_ROLE]
IAM_ROLE_NAME=
ARN=

[S3]
BUCKET=

[AWS]
KEY=
SECRET=
```

## 2. Run the project Notebook

The notebook contains an exploration of the data and generates `.parquet` files for the immigration data.

## 3. Copy Data to S3

I created a Python script to upload the unprocessed data from the local Udacity workspace to an S3 Bucket on AWS. This includes the `.parquet` files created when the `Capstone_Project.ipynb` notebook is run. 

To copy the data, from the Udacity workspace, run:

```bash
python ./copy_data_to_s3.py
```

## 4. Create the Redshift Cluster

Run the script to create a redshift cluster (make sure that the `.cfg` file is complete)

```bash
python ./create_clutster.py
```

Take note of the `IAM_ROLE`, `ARN` and `ENDPOINT` which are needed to complete the `.cfg` and to configure Airflow.

## 5. Database Schema

Several tables are created in this project. The schema is shown in Figure 1.

The Database comprises:
* `public.staging_immigration`: immgration data
* `public.staging_temperature`: world temperature data
* `public.staging_airports`: airport code data
* `public.staging_demographics`: city demographic data
* `public.dim_i94cit`: city codes
* `public.dim_i94port`: port of entry code
* `public.dim_i94mode`: mode of transport codes
* `public.dim_i94addr`: state codes
* `public.dim_i94visa`: vida type codes

![Schema](./images/schema.png#center) 
Figure 1: The Schema modeled in this project.

## 6. ETL Pipeline

The ETL Pipeline is written as an Airflow DAG and shown in Figure 2. The pipeline:
* Creates the dimension tables on Redshift.
* Creates the staging tables on Redshift.
* Copies the data from S3 into the staging tables.
* Copies data from the SAS Labels file into dimension tables.
* Performs Data Quality Checks on tables.

![Pipeline](./images/pipeline.png#center) 
Figure 2: The Airflow DAG ETL Pipeline.

Airflow needs to be configured with the Redshift Connection and AWS Credentials.
Under `Admin > Connections > Create New` add the following 2 connections. The entries should
match those used to make the redshift cluster in `.cfg`.

### AWS Connection

```bash
Conn ID = aws_credentials
Conn Type = Amazon Web Services
Login = <AWS KEY>
Password = <AWS SECRET KEY>
```

### Redshift Connection
```bash
Conn ID = redshift
Conn Type = Postgred
Schema = <DB_NAME>
Login = <DB_USER>
Password = <DB_PASSWORD>
Port = 5439
```