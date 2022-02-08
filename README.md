Let's take a look at how to dump data of Cassandra DB . We have a example about log a ThingsBoard data from Cassandra.

EC2 type : t3.xlarge

Operating System : Ubuntu 18.04 LTS

openjdk version : "1.8.0_292"

Cassandra version :3.11.2.

python version :3.6.9

DB table : ThingsBoard schema

The same model if need to add a data into DB, execute API "session.execute('INSERT INTO table_name(field_1,field_2....) VALUES (value)')",
e.g. 
Take ThingsBoard data for example


....


    session.execute('USE thingsboard')

    query = "INSERT INTO ts_kv_latest_cf (entity_type, entity_id, key, long_v, ts) VALUES ('{}', {}, '{}', {}, {})".format("DEVICE", "fd9996f0-5bf1-11ec-9683-d922572fb795", "key-value", 1234,int(time.time()*1000))
    
    try:
    
        session.execute(query)
        
    except Exception as e:
    
        print(e)
