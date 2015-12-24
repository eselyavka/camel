WITH changedir AS (WITH exclude_repeated_calls AS (WITH all_changedir AS (WITH number_grouping AS
                                                                            (SELECT DISTINCT to_timestamp(t1.idp_recieved) AS ce_ts,
                                                                                             t1.calling_number,
                                                                                             t1.called_number,
                                                                                             t1.event_type_bscm,
                                                                                             t1.service_key,
                                                                                             t1.ext_basic_service_code
                                                                             FROM camel_data t1
                                                                             JOIN camel_data t2 ON t1.calling_number=t2.called_number
                                                                             AND t1.called_number = t2.calling_number)
                                                                          SELECT ce_ts,
                                                                                 calling_number,
                                                                                 called_number,
                                                                                 event_type_bscm,
                                                                                 service_key,
                                                                                 ext_basic_service_code,
                                                                                 calling_number::bigint-called_number::bigint AS group_num
                                                                          FROM number_grouping
                                                                          GROUP BY ce_ts,
                                                                                   calling_number,
                                                                                   called_number,
                                                                                   event_type_bscm,
                                                                                   service_key,
                                                                                   ext_basic_service_code)
                                                   SELECT ce_ts,
                                                          calling_number,
                                                          called_number,
                                                          event_type_bscm,
                                                          service_key,
                                                          ext_basic_service_code,
                                                          group_num,
                                                          ce_ts - lag(ce_ts,1) OVER (w) < '@REPLACEMENT@ seconds' AS deny_period
                                                   FROM all_changedir WINDOW w AS (PARTITION BY group_num
                                                                                   ORDER BY ce_ts)
                                                   ORDER BY ce_ts,
                                                            calling_number)
                   SELECT ce_ts,
                          calling_number,
                          called_number,
                          event_type_bscm,
                          service_key,
                          ext_basic_service_code,
                          group_num
                   FROM exclude_repeated_calls
                   WHERE (NOT deny_period
                          OR deny_period IS NULL))
SELECT ce_ts,
       calling_number,
       called_number,
       event_type_bscm,
       service_key,
       ext_basic_service_code,
       ce_ts - lag(ce_ts,1) OVER (w) < '@REPLACEMENT@ seconds' AS deny_period
FROM changedir WINDOW w AS (PARTITION BY abs(group_num)
                            ORDER BY ce_ts)
ORDER BY ce_ts,
         calling_number
