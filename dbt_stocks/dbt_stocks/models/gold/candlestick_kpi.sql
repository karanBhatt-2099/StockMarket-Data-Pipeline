WITH enriched AS (
    SELECT
        symbol,
        CAST(market_timestamp AS DATE) AS trade_date,
        day_low,
        day_high,
        current_price,
        FIRST_VALUE(current_price) OVER (
            PARTITION BY symbol, trade_date
            ORDER BY market_timestamp
        ) AS candle_open,
        LAST_VALUE(current_price) OVER (
            PARTITION BY symbol, trade_date
            ORDER BY market_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS candle_close
    FROM {{ ref('Silver_stage') }}
),

candles AS (
    SELECT
        symbol,
        trade_date AS candle_time,
        MIN(day_low) AS candle_low,
        MAX(day_high) AS candle_high,
        MIN(candle_open) AS candle_open,
        MIN(candle_close) AS candle_close,
        AVG(current_price) AS trend_line
    FROM enriched
    GROUP BY symbol, trade_date
),

ranked AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY candle_time DESC
        ) AS rn
    FROM candles c
)

SELECT
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
FROM ranked
WHERE rn <= 12
ORDER BY symbol, candle_time
