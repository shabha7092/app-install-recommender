IMPORT '$MACRO_PATH/extract_user_feature.macro';

conversion_data = LOAD '$APP_INSTALL_CONVERSION_DATA' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

click_data = LOAD '$APP_INSTALL_CLICK_DATA' USING org.apache.pig.piggybank.storage.avro.AvroStorage();

user_engagement_data = LOAD '$USER_ENGAGEMENT_DATA' USING OrcStorage();

group_conversion_data = GROUP conversion_data BY advertiser_acct_id;

conversion_count = FOREACH group_conversion_data GENERATE FLATTEN(group) AS conv_advertiser_acct_id:long, COUNT(conversion_data) AS conv_count;

click_data = FOREACH (JOIN click_data BY advertiser_acct_id, conversion_count BY conv_advertiser_acct_id) GENERATE advertiser_acct_id, cmpgn_id, bid, sid, mobile_id, ad_id, receive_time, user_ip_address, x_forwarded_by_ip, user_agent, conv_count;

click_conversion = JOIN click_data BY (x_forwarded_by_ip, ad_id, user_agent) LEFT OUTER, conversion_data BY (x_forwarded_by_ip, ad_id, user_agent);

unmapped_click = FOREACH (FILTER click_conversion BY conversion_data::x_forwarded_by_ip IS NULL AND conversion_data::ad_id IS NULL AND conversion_data::user_agent IS NULL) GENERATE click_data::advertiser_acct_id AS advertiser_acct_id:long, click_data::cmpgn_id AS cmpgn_id:long, click_data::bid AS bid:chararray, click_data::sid AS sid:chararray, click_data::mobile_id AS mobile_id:chararray, click_data::ad_id AS ad_id:long, click_data::receive_time AS receive_time:long, click_data::x_forwarded_by_ip AS x_forwarded_by_ip:chararray, click_data::user_agent AS user_agent:chararray, conv_count;

unmapped_click = FOREACH (GROUP unmapped_click BY advertiser_acct_id) GENERATE FLATTEN(unmapped_click), ((DOUBLE) MAX(unmapped_click.conv_count)/COUNT(unmapped_click.bid) > 1.0 ? 1.0 : (DOUBLE) MAX(unmapped_click.conv_count)/COUNT(unmapped_click.bid)) AS sampling_factor;

sampled_unmapped_clicks = FOREACH (GROUP unmapped_click BY advertiser_acct_id) GENERATE FLATTEN(sampling.SimpleRandomSample(unmapped_click));

conversion_click = FOREACH conversion_data GENERATE advertiser_acct_id, cmpgn_id, bid AS cbid:chararray, sid, mobile_id, ad_id, receive_time, x_forwarded_by_ip, user_agent, 1.0 AS bias:double, 1.0 AS label:double;

unmapped_click = FOREACH sampled_unmapped_clicks GENERATE advertiser_acct_id, cmpgn_id, bid AS cbid:chararray, sid, mobile_id, ad_id, receive_time, x_forwarded_by_ip, user_agent, 1.0 AS bias:double, 0.0 AS label:double;

union_click = UNION ONSCHEMA conversion_click, unmapped_click;

user_features = extract_user_feature(union_click, user_engagement_data);

STORE user_features INTO '$APP_INSTALL_LABEL_DATA' USING org.apache.pig.piggybank.storage.avro.AvroStorage();
