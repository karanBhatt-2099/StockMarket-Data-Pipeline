WITH source AS (
SELECT
    symbol,
    TRY_CAST(current_price AS DOUBLE) AS current_price_dbl,
    market_timestamp
FROM {{ ref('Silver_stage') }}
WHERE TRY_CAST(current_price AS DOUBLE) IS NOT NULL
),

latest_day AS (
SELECT CAST(MAX(market_timestamp) AS DATE) AS max_day
FROM source
),

latest_prices AS (
SELECT
    s.symbol,
    AVG(s.current_price_dbl) AS avg_price
FROM source s
JOIN latest_day ld
    ON CAST(s.market_timestamp AS DATE) = ld.max_day
GROUP BY s.symbol
),

all_time_volatility AS (
SELECT
    symbol,
    STDDEV_POP(current_price_dbl) AS volatility,
    STDDEV_POP(current_price_dbl) / NULLIF(AVG(current_price_dbl), 0) AS relative_volatility
FROM source
GROUP BY symbol
)

SELECT
lp.symbol,
lp.avg_price,
v.volatility,
v.relative_volatility
FROM latest_prices lp
JOIN all_time_volatility v
ON lp.symbol = v.symbol
ORDER BY lp.symbol
