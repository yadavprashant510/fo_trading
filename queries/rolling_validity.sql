SELECT
    trade_id,
    trade_date,
    STDDEV(
        close
    ) OVER (
        PARTITION BY
            i.symbol
        ORDER BY t.trade_date, t.trade_id ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    ) AS rolling_vol
FROM trades t
    JOIN instruments i ON t.instrument_id = i.instrument_id
WHERE
    i.symbol = 'NIFTY'
ORDER BY t.trade_date, t.trade_id;