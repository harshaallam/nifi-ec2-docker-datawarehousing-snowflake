--create a stored procedure to sequentially execute one code after the other and call this procedure by creating a task
--code1 -> checks for change of data between customer_raw and customer before loading or updating into customer (from              customer_raw). Gone for inserting into customer_history first, because once data gets updated into customer,            then we cannot capture historical data(the data which is before updating is called historical data). So we              are comparing existing data in customer with the yet to update into customer(by customer_raw).
--code2-> Now, merging into customer table(insert or update) which works with Change Data Capture principle
--code3-> Deleted records insert into customer_history table
--code4-> Truncating customer_raw, to make sure it doesn't has duplicate records whenever loads using snowpipe.                   Moreover, the data in customer_raw is already loaded into customer, so need of keeping it.
-- Stream resets whenever the data in it gets consumed, same happens with nifi_stream_table as well.

create or replace procedure nifi_realtime.data_stream.nifi_stored_prcdr()
returns string not null
language javascript
as
    $$
     var code1=`
                insert into customer_history  
                (customer_id,first_name,last_name,email,street,city,state,
                country,start_time,end_time,is_current,dml_type)
    
                (select c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,
                c.state,c.country,c.update_timestamp::timestamp_ntz,current_timestamp::timestamp_ntz,False,'Update'                     from customer c
                join customer_raw cr
                on c.customer_id=cr.customer_id
                where c.first_name <> cr.first_name or 
                    c.last_name <> cr.last_name or
                    c.email <> cr.email or
                    c.street <> cr.street or
                    c.city <> cr.city or
                    c.state <> cr.state or
                    c.country <> cr.country);
            `;
    var code2=`
                merge into nifi_realtime.data_stream.customer as c
                using nifi_realtime.data_stream.customer_raw as cr
                on c.customer_id = cr.customer_id
                when matched  and 
                       (c.first_name <> cr.first_name or 
                        c.last_name <> cr.last_name or
                        c.email <> cr.email or
                        c.street <> cr.street or
                        c.city <> cr.city or
                        c.state <> cr.state or
                        c.country <> cr.country)
                
                    then update
                
                    set c.customer_id=cr.customer_id, c.first_name=cr.first_name, c.last_name=cr.last_name,                                  c.email=cr.email, c.street=cr.street, c.city=cr.city, c.state=cr.state, c.country=cr.country,                           c.update_timestamp=current_timestamp()
                
                when not matched then
                    insert (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country) values
                    (cr.customer_id, cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
                `;
    var code3=`
                    insert into customer_history                                                                  (customer_id,first_name,last_name,email,street,city,state,
                    country,start_time,end_time,is_current,dml_type)
            (select   st.customer_id,st.first_name,st.last_name,st.email,st.street,st.city,st.state,
            st.country,st.update_timestamp::timestamp_ntz,current_timestamp::timestamp_ntz,False,'Delete'
            from nifi_stream_table st
            left join customer_history ch
            on st.customer_id=ch.customer_id
            where (st.metadata$action='DELETE' and st.metadata$isupdate=FALSE
                    and 
                    (ch.customer_id is null or
                        (
                            ch.customer_id=st.customer_id
                            and 
                            (
                               ch.last_name<>st.last_name or
                                ch.email <> st.email or
                                 ch.street <> st.street or
                                 ch.city <> st.city or
                                ch.state <> st.state or
                                ch.country <> st.country 
                            )
                        )
                    )
            ));
            `;
    var code4=`truncate table nifi_realtime.data_stream.customer_raw;`;
        var sql1=snowflake.createStatement({sqlText:code1});
        var sql2=snowflake.createStatement({sqlText:code2});
        var sql3=snowflake.createStatement({sqlText:code3});
        var sql4=snowflake.createStatement({sqlText:code4});
        sql1.execute();
        sql2.execute();
        sql3.execute();
        sql4.execute();
        return 'Successfully Executed';
    $$;
        
    

--create a task and call stored procedure in it by mentioning a frequency
create or replace task nifi_realtime.data_stream.nifi_task
warehouse='compute_wh'
schedule='10 minutes'
as 
call nifi_realtime.data_stream.nifi_stored_prcdr();

--change it resume or suspend to start the task
alter task nifi_realtime.data_stream.nifi_task resume;

select * from nifi_realtime.data_stream.customer_raw;
select * from nifi_realtime.data_stream.customer;
select * from nifi_realtime.data_stream.customer_history;