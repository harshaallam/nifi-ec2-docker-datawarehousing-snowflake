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
- So, we need ec2 instance and run a docker on top of it. Create an ec2 instance, and connect through terminal or command prompt using SSH method.
- Inside the docker, compose .yml file (docker image) that has code to set up applications with specific ports to access respective applications.
- Create a directory to store `docker-compose.yml` file and copy the whole to ec2 level from local system, so that docker can access the .yml file. 
- Refer [`docker_installation.sh`](./docker_installation.sh) file to install docker and copy .yml's directory into ec2.
- Refer [`docker-compose.yml`](./docker-compose.yml) for composing a docker image and creating containers.
- Once container set for jupyer notebook and Apache Nifi, connect to them using a web browser.
- Enter 'http://ipaddress-of-ec2:port-address'
- port-address is available in .yml file.

  **Genrating faker files**
  - As we don't have access to real-time streaming data of any organization, we can write python code to generate faker csv files.
  - The files can be accessed by Apache Nifi through shared path provided in [`docker-compose.yml`](./docker-compose.yml) file.
  - Set up a group of processors in Apache Nifi. That inlcudes three processors to list file, fetch file, and put file into S3.
  - Connect the processors and run it list, fetch and finally place the file into S3 bucket.

   **Snowflake**
  - Create three tables `customer_raw`, `customer`, `customer_history`. `Customer_raw` is a staging table and stores raw data.
  - Create file format, stage, stream and snowpipe.
  - Create a notification event in S3 bucket and establish connection with snowpipe. This event notifies snowpipe on a csv file availability and therefore pipe trigger's to load data into `customer_raw` table
  - Prepare queries to make incremenatal data load into `customer` and historical data into `customer_history`.
  - Create a stored procedure and automate it using by creating a snowflake task.
  - Refer [`nifi_table_creations.sql`](./nifi_table_creations.sql) , [`nifi_stored_procedure.sql`](./nifi_stored_procedure.sql), and [`nifi_analysis.sql`](./nifi_analysis.sql) for SQL code.
