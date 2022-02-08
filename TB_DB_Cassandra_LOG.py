from cassandra.cluster import Cluster
from datetime import datetime
from time import gmtime
import time
import json  
if __name__ == "__main__":
    cluster = Cluster(['xxx.xxx.xxx.220'],port=9042)
    session = cluster.connect('thingsboard',wait_for_all_pools=True)
    session.execute('USE thingsboard')
    with open("TB_DBLOG_CONFIG.json","r") as c:
      INFO= json.load(c)    

    while True:
      rows = session.execute('SELECT * FROM ts_kv_latest_cf')
      for row in rows:
        unix_timestamp = int(row.ts)
        utc_time = time.gmtime(unix_timestamp/1000.0)
        local_time = time.localtime(unix_timestamp/1000.0)

        if INFO["KEY"] =='ALL':
          if time.strftime("%Y-%m-%d",local_time) == INFO["TIME"] and str(row.entity_id)== INFO["DEVICE_ID"]:
            date = time.strftime("%Y-%m-%d %H:%M:%S+00:00 (UTC)",utc_time)
            #print('entity_id:',row.entity_id)
            print('key:',row.key)
            if str(row.long_v)!='None':
              print(' long_v:',row.long_v)
            if str(row.str_v)!='None':
              print(' str_v:',row.str_v) 
            print(' date:',date,"\n")             
          else : 
            date = time.strftime("%Y-%m-%d %H:%M:%S",local_time)  
        else :
          if time.strftime("%Y-%m-%d",local_time) ==INFO["TIME"] and str(row.entity_id)== INFO["DEVICE_ID"] and str(row.key)== INFO["KEY"]:
            date = time.strftime("%Y-%m-%d %H:%M:%S+00:00 (UTC)",utc_time)
            print('key:',row.key)
            if str(row.long_v)!='None':
              print(' long_v:',row.long_v)
            if str(row.str_v)!='None':
              print(' str_v:',row.str_v) 
            print(' date:',date,"\n")          
    time.sleep(2)
     
    

