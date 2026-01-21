WITH
    oi_diff AS (
        SELECT
            i.symbol,
            e.exchange_name,
            trade_date,
            open_int - LAG(open_int) OVER (
                PARTITION BY
                    i.symbol,
                    e.exchange_name
                ORDER BY trade_date
            ) AS oi_change
        FROM
            trades t
            JOIN instruments i ON t.instrument_id = i.instrument_id
            JOIN exchanges e ON i.exchange_id = e.exchange_id
    )
SELECT
    symbol,
    exchange_name,
    SUM(oi_change) total_oi_change
FROM oi_diff
GROUP BY
    symbol,
    exchange_name
ORDER BY total_oi_change DESC
LIMIT 10;
