click_data  = LOAD '$CLICK_DATA' USING PigStorage('\t', '-schema');

click_data = FILTER click_data BY advertiser_acct_id IN (1589950L, 1624400L, 1630090L, 1630130L, 1630141L, 1630122L) AND advertiser_acct_id IS NOT NULL AND bcookie IS NOT NULL AND x_forwarded_by_ip IS NOT NULL;

click_data = FOREACH click_data GENERATE advertiser_acct_id, cmpgn_id, bcookie AS bid, sid, mobile_id, ad_id, receive_time, user_ip_address, x_forwarded_by_ip, user_agent; 

click_data = DISTINCT click_data;

group_click_data = GROUP click_data BY (x_forwarded_by_ip, ad_id);

click_data = FOREACH group_click_data  {
     sort_receive_time = ORDER click_data BY receive_time ASC;
     click_row = LIMIT sort_receive_time 1; 
     GENERATE FLATTEN(click_row) AS (advertiser_acct_id:long, cmpgn_id:long, bid:chararray, sid:chararray, mobile_id:chararray, ad_id:long, receive_time:long, user_ip_address:chararray, x_forwarded_by_ip:chararray, user_agent:chararray);
     };

STORE click_data INTO '$APP_INSTALL_CLICK_DATA' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
