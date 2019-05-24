# Summary of the project
 The goal of this project is to develop an ETL pipeline tool for Sparkify that extracts song data from S3, transforms data into a set of dimensional tables using Spark, and loads the output files on S3 for an analytics team to continue finding insights in what songs their users are listening to.

[Fact and Dimension table schema](https://drive.google.com/file/d/1DeIz6PCJRfsi_hqoOLTC3QLRJRrxVfFd/view?usp=sharing)

### Instruction to run the tool from python console

```sh
$ python etl.py
```

Code explaination

```sh
dwh.cfg - this file has all of the configuration settings in it (S3 storage information)
etl.py - this script file will load aws configuration, create a spark session, process song data, and log data to extract, transform, and load the output for additional analysis
etl.ipynb - this is a python notebook used during development; has sample code that can modified to test changes
```