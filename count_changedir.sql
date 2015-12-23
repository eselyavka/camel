WITH count_changedir AS (WITH changedir AS (WITH number_grouping AS
                                              (SELECT DISTINCT to_timestamp(t1.idp_recieved) AS ce_ts,
                                                               t1.calling_number,
                                                               t1.called_number
                                               FROM camel_data t1
                                               JOIN camel_data t2 ON t1.calling_number=t2.called_number
                                               AND t1.called_number = t2.calling_number)
                                            SELECT ce_ts,
                                                   calling_number,
                                                   called_number,
                                                   abs(calling_number::bigint-called_number::bigint) AS group_num
                                            FROM number_grouping
                                            GROUP BY ce_ts,
                                                     calling_number,
                                                     called_number)
                         SELECT calling_number,
                                called_number,
                                ce_ts,
                                ce_ts - lag(ce_ts,1) OVER (w) < '@REPLACEMENT@ seconds' AS deny_period
                         FROM changedir WINDOW w AS (PARTITION BY group_num
                                                     ORDER BY ce_ts)
                         ORDER BY ce_ts,
                                  calling_number)
SELECT count(*)
FROM count_changedir
WHERE deny_period
