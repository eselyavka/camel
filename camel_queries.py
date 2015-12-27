CHANGE_DIR = """WITH changedir AS (WITH number_grouping AS
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
                          power(calling_number::bigint,2) + power(called_number::bigint,2) AS group_num
                   FROM number_grouping
                   WHERE calling_number != called_number)
SELECT ce_ts,
       calling_number,
       called_number,
       event_type_bscm,
       service_key,
       ext_basic_service_code,
       group_num,
       ce_ts - lag(ce_ts,1) OVER (w) < '@REPLACEMENT@ seconds' AS deny_period
FROM changedir WINDOW w AS (PARTITION BY group_num
                            ORDER BY ce_ts)
ORDER BY ce_ts,
         calling_number
"""

COUNT_CHANGEDIR = """WITH count_changedir AS (""" + CHANGE_DIR + """)
SELECT count(*)
FROM count_changedir
WHERE deny_period
"""

ALL_EXCEPT_CHANGEDIR = """WITH all_except_changedir AS (""" + CHANGE_DIR + """)
SELECT to_timestamp(cd.idp_recieved),
       cd.calling_number,
       cd.called_number,
       cd.event_type_bscm,
       cd.service_key,
       cd.ext_basic_service_code
FROM camel_data cd
EXCEPT
SELECT ex.ce_ts,
       ex.calling_number,
       ex.called_number,
       ex.event_type_bscm,
       ex.service_key,
       ex.ext_basic_service_code
FROM all_except_changedir ex
WHERE ex.deny_period
"""
FILTER_CHANGEDIR = """WITH filter_changedir AS (""" + CHANGE_DIR + """)
SELECT ce_ts,
       calling_number,
       called_number,
       event_type_bscm,
       service_key,
       ext_basic_service_code
FROM filter_changedir
WHERE deny_period
"""
