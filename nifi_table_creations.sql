--create three tables customer_raw(staging), customer(target), and customer_history(stores historical data)

CREATE  OR REPLACE DATABASE nifi_realtime;
CREATE OR REPLACE SCHEMA data_stream;

create or replace table nifi_realtime.data_stream.customer(
customer_id integer,
first_name varchar,
last_name varchar,
email varchar,
street varchar,
city varchar,
state varchar,
country varchar,
update_timestamp timestamp_ntz default current_timestamp
);

create or replace table nifi_realtime.data_stream.customer_history(
customer_id integer,
first_name varchar,
last_name varchar,
email varchar,
street varchar,
city varchar,
state varchar,
country varchar,
start_time timestamp_ntz default current_timestamp,
end_time timestamp_ntz default current_timestamp,
is_current boolean,
dml_type varchar
);

create or replace table nifi_realtime.data_stream.customer_raw(
customer_id integer,
first_name varchar,
last_name varchar,
email varchar,
street varchar,
city varchar,
state varchar,
country varchar
);


--create file format
create or replace file format nifi_realtime.data_stream.nifi_file
TYPE=CSV
FIELD_DELIMITER=','
SKIP_HEADER=1
FIELD_OPTIONALLY_ENCLOSED_BY='"';

--create stage for building connection between snowflake and aws(IAM user)
--we can also use storage integration to avoid entering keys
CREATE OR REPLACE STAGE nifi_realtime.data_stream.nifi_stage
URL='s3://dw-snowflake-de/nifi-realtime-stream/'
credentials=(aws_key_id='#',aws_secret_key='#')
file_format=nifi_file;

--create a pipe to automate data load when file arrives
create or replace pipe nifi_pipe
auto_ingest=True
as
copy into nifi_realtime.data_stream.customer_raw
from @nifi_realtime.data_stream.nifi_stage;

--create a notification event s3 bucket by entering pipe's notification channel value in SQS ARN, to nofiy snowpipe when a file is placed in s3 bucket
show pipes;

list @nifi_realtime.data_stream.nifi_stage;

--create a stream to capture any changes takes place in customer table, stream gets reset once the data is consumed by a query
create or replace stream nifi_realtime.data_stream.nifi_stream_table on table customer;