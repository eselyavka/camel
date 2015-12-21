WITH changedir AS
  (SELECT DISTINCT to_timestamp(t1.idp_recieved) AS idp_ts,
                   t1.calling_number,
                   t1.called_number
   FROM camel_data t1
   JOIN camel_data t2 ON t1.calling_number=t2.called_number
   AND t1.called_number = t2.called_number)
SELECT calling_number,
       called_number,
       idp_ts,
       idp_ts - lag(idp_ts,1) OVER (w) < '30 minutes' AS deny_period
FROM changedir WINDOW w AS (PARTITION BY calling_number
                            ORDER BY idp_ts)
ORDER BY calling_number,
         idp_ts;
