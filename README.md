# nifi-ec2-docker-datawarehousing-snowflake

This project's agenda is to build an architure to capture and make use of the data of real-time data streaming files. Automate the process of incremental data load and store historical data in Snowflake DataWarehouse.

![Architechture](Nifi_Snowflake_Architecture.png)

## Requried tools:
- AWS EC2 instance
- Docker
- Snowflake

### Business requirement:
- To consume data of real-time streaming files, and do incremental data load into customer table and store historical data in customer_history table. If a new file has records with cusotmer_id that is not present in customer table, then it has to be an insert. Else, should be an update into the customer table. The older value of custome_id in customer table should be stored in customer_history table as this would be considered as historical data. In the similar way it should store deleted records.

### Setting up infrastructure:
- We need jupyter notebook to write code and generate files, and Apache Nifi to capture and stores the files in S3 bucket.
- So, we need ec2 instance and run a docker on top of it. Inside the docker, compose .yml file (docker image) that has code to set up applications with their respective ports to access.
- Refer 
