DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

click_data = LOAD '$APP_INSTALL_CLICK_DATA' USING  org.apache.pig.piggybank.storage.avro.AvroStorage();

conversion_data = LOAD '$CONVERSION_DATA' USING PigStorage('\t');

project_conversion_data = FOREACH conversion_data GENERATE (long) $0 AS projectId, (long) $3 AS receiveTime:long, (chararray) $4 AS userIp:chararray, com.shabha.app.grid.udf.ExtractAdId($5) As adId:long, com.shabha.app.grid.udf.ExtractUserAgent($6) AS userAgent:chararray, (double) $7 AS convProb:double, (chararray) $18 AS deviceIdType, (chararray) $19 AS deviceId;

filter_conversion_data = FILTER project_conversion_data BY receiveTime > (long) ISOToUnix('$CONVERSION_FILTER_START_DATE') AND receiveTime < (long) ISOToUnix('$CONVERSION_FILTER_END_DATE') AND adId != -1L AND userAgent != '' AND convProb >= 0.5;

filter_conversion_data = FOREACH filter_conversion_data GENERATE receiveTime, userIp, adId, userAgent, convProb, deviceIdType, deviceId;

join_data = JOIN click_data BY (x_forwarded_by_ip, ad_id) FULL OUTER, filter_conversion_data BY (userIp, adId);

mapped_clicks  = FILTER join_data BY x_forwarded_by_ip IS NOT NULL AND userIp IS NOT NULL;

unmapped_click_data = FILTER join_data BY userIp IS NULL AND x_forwarded_by_ip IS NOT NULL;

unmapped_conversion_data = FILTER join_data BY x_forwarded_by_ip IS NULL AND userIp IS NOT NULL;

project_unmapped_click_data = FOREACH unmapped_click_data GENERATE advertiser_acct_id, cmpgn_id, bid, sid, mobile_id, ad_id, receive_time, x_forwarded_by_ip, user_agent;

project_unmapped_conversion_data = FOREACH unmapped_conversion_data GENERATE receiveTime, userIp, adId, userAgent, convProb, deviceIdType, deviceId;

unmapped_click_bag = GROUP project_unmapped_click_data ALL;

unmapped_conversion_click = FOREACH project_unmapped_conversion_data  GENERATE TOTUPLE(receiveTime, userIp, adId, userAgent, convProb, deviceIdType, deviceId) AS conversion_tuple:tuple(receiveTime:long, userIp:chararray, adId:long, userAgent:chararray, convProb:double, deviceIdType:chararray, deviceId:chararray), unmapped_click_bag.project_unmapped_click_data AS click_bag:bag{(advertiser_acct_id: long, cmpgn_id: long, bid: chararray, sid: chararray, mobile_id: chararray, ad_id: long, receive_time: long, x_forwarded_by_ip: chararray, user_agent: chararray)};

filter_unmapped_conversion_click =  FOREACH unmapped_conversion_click GENERATE FLATTEN(com.shabha.app.grid.udf.FilterClickBag(conversion_tuple, click_bag)) AS (conversion_tuple:tuple(receiveTime:long, userIp:chararray, adId:long, userAgent:chararray, convProb:double, deviceIdType:chararray, deviceId:chararray), click_bag:bag{(advertiser_acct_id: long, cmpgn_id: long, bid: chararray, sid: chararray, mobile_id: chararray, ad_id: long, receive_time: long, x_forwarded_by_ip: chararray, user_agent: chararray)});

filter_unmapped_conversion_click = FILTER filter_unmapped_conversion_click BY NOT IsEmpty(click_bag);

unmapped_click_ipv4_conversion = FILTER filter_unmapped_conversion_click BY com.shabha.app.grid.udf.IsValidIPv4(conversion_tuple.userIp);

closest_ipv4_click = FOREACH unmapped_click_ipv4_conversion GENERATE conversion_tuple, com.shabha.app.grid.udf.ExtractClosestIPV4(conversion_tuple, click_bag) AS (click_tuple:tuple(advertiser_acct_id: long, cmpgn_id: long, bid: chararray, sid: chararray, mobile_id: chararray, ad_id: long, receive_time: long, x_forwarded_by_ip: chararray, user_agent: chararray));

flatten_closest_ipv4_click = FOREACH closest_ipv4_click GENERATE FLATTEN(click_tuple) AS (advertiser_acct_id: long, cmpgn_id: long, bid: chararray, sid: chararray, mobile_id: chararray, ad_id: long, receive_time: long, x_forwarded_by_ip: chararray, user_agent: chararray), FLATTEN(conversion_tuple) AS (receiveTime: long, userIp: chararray, adId: long, userAgent: chararray, convProb: double, deviceIdType: chararray, deviceId:chararray);

unmapped_click_ipv6_conversion = FILTER filter_unmapped_conversion_click BY NOT com.shabha.app.grid.udf.IsValidIPv4(conversion_tuple.userIp);

closest_ipv6_click = FOREACH unmapped_click_ipv6_conversion GENERATE conversion_tuple, com.shabha.app.grid.udf.ExtractClosestIPV6(conversion_tuple, click_bag) AS (click_tuple:tuple(advertiser_acct_id: long, cmpgn_id: long, bid: chararray, sid: chararray, mobile_id: chararray, ad_id: long, receive_time: long, x_forwarded_by_ip: chararray, user_agent: chararray));

flatten_closest_ipv6_click = FOREACH closest_ipv6_click GENERATE FLATTEN(click_tuple) AS (advertiser_acct_id: long, cmpgn_id: long, bid: chararray, sid: chararray, mobile_id: chararray, ad_id: long, receive_time: long, x_forwarded_by_ip: chararray, user_agent: chararray), FLATTEN(conversion_tuple) AS (receiveTime: long, userIp: chararray, adId: long, userAgent: chararray, convProb: double, deviceIdType: chararray, deviceId:chararray);

mapped_clicks = FOREACH mapped_clicks GENERATE advertiser_acct_id, cmpgn_id, bid, sid, mobile_id, ad_id, receive_time, x_forwarded_by_ip, user_agent, receiveTime, userIp, adId, userAgent, convProb, deviceIdType, deviceId;

-- mapped_data = UNION ONSCHEMA mapped_clicks, flatten_closest_ipv4_click, flatten_closest_ipv6_click;

mapped_data = UNION ONSCHEMA mapped_clicks, flatten_closest_ipv4_click;

polka_data = FOREACH mapped_clicks GENERATE bid, sid, deviceId AS device_Id:chararray, deviceIdType AS device_id_type:chararray, receive_time AS impression_timestamp:long, receiveTime AS event_timestamp:long;

STORE mapped_data INTO '$APP_INSTALL_CONVERSION_DATA' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

STORE polka_data INTO '$MAPPED_DATA' USING PigStorage('\t', '-schema');
