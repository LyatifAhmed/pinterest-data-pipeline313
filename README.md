# AiCore Project pinterest-data-pipeline

#### Project description
Build Pinterest's experimental data pipeline. Pinterest have billions of user interactions such as image uploads or image clicks 
which they need to process every day to inform the decisions to make. The project builds a system hosted in the cloud 
that ingests both batch and streaming events, and runs them through separate pipelines. The streaming pipeline will be 
used to compute real-time metrics (to recommend a profile in real-time), and the batch pipeline is used for computing 
metrics that depend on historical data.

#### Batch Data Pipeline Flow
Batch data (JSON payload) is sent via HTTP POST requests to an AWS API Gateway. The gateway routes the requests to a Kafka REST proxy, which
publishes them to the MSK Cluster. The MSK cluster writes the data to topics previously configured which are in a S3 bucket.
Databricks is configured to mount the S3 bucket. A databricks notebook runs daily as scheduled in MWAA Scheduler. The notebook 
reads the latest data and runs various queries against it.
#### Streaming Data Pipeline Flow
Streaming data (JSON payload) is sent via HTTP PUT requests to an AWS API Gateway. The gateway routes the requests to Amazon Kinesis Data Streams.
A Databricks Notebook connects to the Kinesis Data Streams, reads, cleans and writes the streaming data to delta tables.
#### Project aim
Put into practice concepts learnt about AWS, Kafka, Spark, Airflow and Databricks. Use python as the central
language across all applications.

For AWS, I configured: IAM roles, EC2 instances, S3 buckets, an MSK Cluster, API Gateways, a MWAA Scheduler and Kinesis Data Streams.
In Kafka, I created topics, used producers and consumers and configured a REST proxy.
Spark was hosted in Databricks. I used pyspark to extract, transform and load data.
For Airflow, I created a DAG to schedule a Databricks Notebook, ran and monitored the job.
I used Databricks to create Notebooks with python & pyspark commands.

#### Tech Stack
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=flat-square&logo=apachespark&logoColor=black)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
#### Description of files
| filename                           | description                                                                                                                 |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| user_posting_emulation.py          | Python file which runs the process to extract data from an AWS database and create HTTP POST messages for batch processing. |
| batch_processing_databricks.py     | Databricks notebook for mounting an S3 bucket and notebook with code to read, clean and query Pinterest batch data.                                                                             |
| 0affcdd81315_dag.py                | Python file. Contains code for MWAA (AWS managed Airflow) to schedule a Databricks notebook run                             |
| stream_processing_databricks.py    | Databricks notebook with code to read and clean Pinterest stream data, and then write it to Delta Tables.                   |
| user_posting_emulation_streaming.py | Python file which runs the process to extract data from an AWS database and create HTTP PUT messages for stream processing. |
| README.md                          | Text file (markdown format) description of the project.                                                                     |



