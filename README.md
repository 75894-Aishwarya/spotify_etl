# spotify_etl


Objective
Develop an ETL pipeline using AWS services to obtain the artist, album, and song information from the Spotify.


The pipeline involves  transforming the data to prepare it for analysis, loading the transformed data into Amazon S3, and querying the transformed data using Amazon Athena, utilizing AWS Glue for cataloging and metadata management.

Tools used: AWS services ( S3, Glue and Athena)

This is a brief overview of what each service does:

S3 (Simple Storage Service): Easily store and retrieve large amounts of data. Each file is called an object and data is stored in buckets.
Lambda: Serverless compute service to run code without managing servers. We will use Lambda to deploy the Python code to perform data extraction and transformation

Crawler: Component of AWS Glue that automatically scans and analyzes data sources to infer their schema and create metadata tables

Glue Data Catalog: Fully managed metadata repository provided by AWS Glue. It acts as a central repository for storing and organizing metadata information about various data sources, including tables, schemas, and partitions. You can use the Glue Data Catalog without the Crawler if you already have the metadata information or prefer to define and manage the metadata manually and can directly create and populate tables in the Glue Data Catalog

Athena: Interactive query service to analyze data stored in various sources using standard SQL queries. You can query data from the Glue Data Catalog, S3 and other supported data sources.

