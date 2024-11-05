--this is more into analysing part of the data

select customer_id,first_name,last_name,email,street,city,country, 
update_timestamp as start_time,
'9999-12-31' as end_time from
(
select customer_id,first_name,last_name,email,street,city,country,update_timestamp 
from nifi_realtime.data_stream.nifi_stream_table 
where metadata$action='INSERT' and metadata$isupdate=True);


create or replace view v_change_data_customer as (
select customer_id,first_name,last_name,email,street,city,state,country,start_time,end_time,is_current, 'I' as dml_type 
from(
select customer_id,first_name,last_name,email,street,city,state,country,
update_timestamp, update_timestamp as start_time,
lag(update_timestamp) over (partition by customer_id order by update_timestamp desc) as end_raw,
case when end_raw is null then '9999-12-31'::timestamp_ntz else end_raw end as end_time,
case when end_raw is null then TRUE else FALSE end as is_current
from  nifi_stream_table 
where metadata$action='INSERT' and metadata$isupdate=False)

union
select customer_id,first_name,last_name,email,street,city,state,country, start_time, end_time, is_current, 'U' as dml_type
from(
 select customer_id,first_name,last_name,email,street,city,state,country,
update_timestamp, update_timestamp as start_time,
lag(update_timestamp) over (partition by customer_id order by update_timestamp desc) as end_raw,
case when end_raw is null then '9999-12-31'::timestamp_ntz else end_raw end as end_time,
case when end_raw is null then TRUE else FALSE end as is_current
from  nifi_stream_table 
where metadata$action='INSERT' and metadata$isupdate=TRUE)
union
select customer_id,first_name,last_name,email,street,city,state,country,
update_timestamp::timestamp_ntz as start_time, update_timestamp::timestamp_ntz as end_time, False as is_current,'D' as dml_type
from nifi_stream_table where metadata$action='DELETE' and metadata$isupdate=False
);

select * from v_change_data_customer where dml_type='D';

 select * from nifi_stream_table
 where metadata$action='DELETE' and metadata$isupdate=False;

desc stream nifi_stream_table;
 
select 
(select lag(update_timestamp) over (partition by update_timestamp order by update_timestamp desc) from nifi_stream_table where metadata$action='DELETE') as start_time,

from nifi_stream_table where );



select customer_id,update_timestamp, metadata$action,metadata$isupdate,first_name,last_name,email,street,city,country from nifi_stream_table 
order by customer_id, update_timestamp desc;


SELECT * FROM V_STREAM WHERE dml_type in ('',NULL);

select (lag(update_timestamp) over (partition by update_timestamp order by update_timestamp) desc as start,
case when start is null then update_timestampe else start end as start_time;

merge into customer_history ch
using v_change_data_customer cdc 
on ch.customer_id=cdc.customer_id and ch.start_time=cdc.start_time
    when matched and cdc.dml_type='U' 
        then 
            update set ch.end_time=cdc.end_time, ch.is_current=False
    when matched and cdc.dml_type='D'
        then 
            update set ch.end_time=cdc.end_time, ch.is_current=False
    when not matched and cdc.dml_type='I'
        then
            insert (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY, start_time, end_time,                    is_current)  values
            (cdc.CUSTOMER_ID, cdc.FIRST_NAME, cdc.LAST_NAME, cdc.EMAIL, cdc.STREET, cdc.CITY,cdc.STATE,cdc.COUNTRY,                  cdc.start_time, cdc.end_time, cdc.is_current);

insert into customer_history (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, start_time, end_time, is_current, dml_type)
select cdc.CUSTOMER_ID, cdc.FIRST_NAME, cdc.LAST_NAME, cdc.EMAIL, cdc.STREET, cdc.CITY, cdc.STATE, cdc.COUNTRY, cdc.start_time, cdc.end_time, cdc.is_current, cdc.dml_type
from v_change_data_customer cdc;

select * from customer_history;