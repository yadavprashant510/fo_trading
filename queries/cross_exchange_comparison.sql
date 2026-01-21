SELECT e.exchange_name, AVG(settle_pr) avg_settle
FROM
    trades t
    JOIN instruments i ON t.instrument_id = i.instrument_id
    JOIN exchanges e ON i.exchange_id = e.exchange_id
WHERE
    e.exchange_name IN ('NSE', 'MCX')
GROUP BY
    e.exchange_name;