DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

click_data = LOAD '$APP_INSTALL_CLICK_DATA' USING  org.apache.pig.piggybank.storage.avro.AvroStorage();

conversion_data = LOAD '$CONVERSION_DATA' USING PigStorage('\t');

project_conversion_data = FOREACH conversion_data GENERATE (long) $0 AS projectId, (long) $3 AS receiveTime:long, (chararray) $4 AS userIp:chararray, com.shabha.app.grid.udf.ExtractAdId($5) As adId:long, com.shabha.app.grid.udf.ExtractBid($5) As fBid:chararray,  com.shabha.app.grid.udf.ExtractUserAgent($6) AS userAgent:chararray, (double) $7 AS convProb:double, (chararray) $18 AS deviceIdType, (chararray) $19 AS deviceId;

filter_conversion_data = FILTER project_conversion_data BY receiveTime > (long) ISOToUnix('$CONVERSION_FILTER_START_DATE') AND receiveTime < (long) ISOToUnix('$CONVERSION_FILTER_END_DATE') AND adId != -1L AND userAgent != '' AND deviceId != '';

filter_conversion_data = DISTINCT filter_conversion_data;

mapped_clicks = JOIN click_data BY bid, filter_conversion_data BY fBid;

mapped_data = FOREACH mapped_clicks GENERATE bid, sid, deviceId AS device_Id:chararray, deviceIdType AS device_id_type:chararray, receive_time AS impression_timestamp:long, receiveTime AS event_timestamp:long;

conversion_data = FOREACH mapped_clicks GENERATE advertiser_acct_id AS advertiser_acct_id:long, cmpgn_id AS cmpgn_id:long, bid AS bid:chararray, sid AS sid:chararray, mobile_id AS mobile_id:chararray, ad_id AS ad_id:long, receive_time AS receive_time:long, x_forwarded_by_ip AS x_forwarded_by_ip:chararray, user_agent AS user_agent:chararray, receiveTime AS receiveTime:long, userIp AS userIp:chararray, adId AS adId: long, userAgent AS userAgent: chararray, convProb AS convProb: double, deviceIdType AS deviceIdType: chararray, deviceId AS deviceId:chararray;

STORE conversion_data INTO '$APP_INSTALL_CONVERSION_DATA' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

STORE mapped_data INTO '$POLKA_DATA' USING PigStorage('\t', '-schema');
