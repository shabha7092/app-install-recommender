impression_data  = LOAD '$IMPRESSION_DATA' USING PigStorage('\t', '-schema')

impression_data = FILTER impression_data BY advertiser_acct_id IN (1589950L, 1624400L, 1630090L, 1630130L, 1630141L, 1630122L) AND advertiser_acct_id IS NOT NULL AND bcookie IS NOT NULL AND x_forwarded_by_ip IS NOT NULL;

impression_data = FOREACH impression_data GENERATE advertiser_acct_id, cmpgn_id, bcookie AS bid, sid, mobile_id, ad_id, receive_time, user_ip_address, x_forwarded_by_ip, user_agent;

impression_data = DISTINCT impression_data;

group_impression_data = GROUP impression_data BY (x_forwarded_by_ip, ad_id);

impression_data = FOREACH group_impression_data  {
     sort_receive_time = ORDER impression_data BY receive_time ASC;
     impression_row = LIMIT sort_receive_time 1; 
     GENERATE FLATTEN(impression_row) AS (advertiser_acct_id:long, cmpgn_id:long, bid:chararray, sid:chararray, mobile_id:chararray, ad_id:long, receive_time:long, user_ip_address:chararray, x_forwarded_by_ip:chararray, user_agent:chararray);     
     };

STORE impression_data INTO '$APP_INSTALL_IMPRESSION_DATA' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
