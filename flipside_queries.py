# Queries used to extract the desired on-chain information from Flipside app


# Time Series
time_series = """
WITH new_users AS  (
  SELECT
    cohort_month,
    COUNT(ORIGIN_FROM_ADDRESS) AS new_traders
  FROM
    (SELECT
      MIN(DATE_TRUNC('month',block_timestamp)) AS cohort_month,
      ORIGIN_FROM_ADDRESS
    FROM
      ethereum.core.ez_dex_swaps
    WHERE
      platform = 'uniswap-v3'
    GROUP BY
      ORIGIN_FROM_ADDRESS
    )
  GROUP BY
    cohort_month

),

gas_price_per_tx AS (
  SELECT
    DATE_TRUNC('month', block_timestamp) as month,
    AVG(gas_price) avg_gas_price
  FROM
    (
      SELECT
            swap.block_timestamp,
            swap.tx_hash,
            origin_from_address,
            tx.gas_price,
            ROW_NUMBER() OVER(PARTITION BY swap.tx_hash order by swap.block_timestamp) as row_id
            
          FROM
            ethereum.core.ez_dex_swaps swap
            LEFT JOIN ethereum.core.fact_transactions tx
              ON (swap.tx_hash = tx.tx_hash AND swap.block_timestamp = tx.block_timestamp)
            
        WHERE
              platform = 'uniswap-v3'
        ORDER BY
          swap.block_timestamp
    )
  WHERE
    row_id = 1
  GROUP BY
     month
),

mau_tab AS (
  SELECT
    DATE_TRUNC('month',swap.block_timestamp) as month,
    COUNT(DISTINCT swap.ORIGIN_FROM_ADDRESS) AS total_traders,
    SUM(AMOUNT_IN_USD) as vol_usd,
    SUM(fee_percent * amount_in_usd) AS fee_usd,
    COUNT(*) as swap_count

  FROM
    ethereum.core.ez_dex_swaps swap
    INNER JOIN ethereum.uniswapv3.ez_pools pool ON swap.contract_address = pool.pool_address
  WHERE
    platform = 'uniswap-v3'

  GROUP BY
    month
),

dau_tab AS (
  SELECT
    DATE_TRUNC('month',day) AS month,
    AVG(total_traders) as dau
  FROM
    (
      SELECT
        DATE(block_timestamp) as day,
        COUNT(DISTINCT ORIGIN_FROM_ADDRESS) AS total_traders
      FROM
        ethereum.core.ez_dex_swaps
      WHERE
        platform = 'uniswap-v3'
      GROUP BY
        day
    )
  GROUP BY
    month
),

-- Federal Funds Rate Series - ⚡️LiveQuery
interest_rate AS (
  WITH fred_data as (
  SELECT
    fred.get_series({
      'series_id': 'FEDFUNDS',
      'file_type': 'json',
      'observation_start': '2021-05-01',
      'frequency': 'm'
    }) as result
  )

  SELECT
    array.value:date::STRING AS month,
    array.value:value::FLOAT AS interest_rate_percent
  FROM
    fred_data,
    LATERAL FLATTEN(input => TO_VARIANT(result:data:observations)) as array
)

SELECT
  DATE(total.month) as month,
  total_traders,
  new_traders,
  ROUND((new_traders/total_traders),3) as new_traders_percent,
  1 - ROUND((new_traders/total_traders),3) as old_traders_percent,
  ROUND(dau/total_traders,3) AS stickiness, -- Stickiness Ratio
  swap_count,
  vol_usd,
  (vol_usd/swap_count) AS avg_tx_value,
  fee_usd as revenue_usd,
  gas.avg_gas_price,
  interest_rate_percent
FROM
  mau_tab total
  LEFT JOIN new_users new ON total.month = new.cohort_month
  LEFT JOIN dau_tab dau ON total.month = dau.month
  INNER JOIN gas_price_per_tx gas on total.month = gas.month
  INNER JOIN interest_rate rate ON total.month = rate.month
ORDER BY
  total.month ASC
"""


#Cohort Analysis
cohort = """
-- Setting the first cohort and date for each trader
WITH user_cohort AS (
    SELECT
      MIN(DATE_TRUNC('month',block_timestamp)) AS cohort_month,
      origin_from_address
    FROM
      ethereum.core.ez_dex_swaps
    WHERE
      platform = 'uniswap-v3'
    GROUP BY
      ORIGIN_FROM_ADDRESS
),

-- Retrieving the metrics for each user giving a cohort
following_month AS (
  SELECT
    DATEDIFF('month', uc.cohort_month, DATE_TRUNC('month', s.block_timestamp)) as cohort_ID,
    s.origin_from_address,
    SUM(AMOUNT_IN_USD) as vol_usd,
    SUM(fee_percent * amount_in_usd) AS fee_usd,
    COUNT(*) as swap_count
    
  FROM
    ethereum.core.ez_dex_swaps s
    LEFT JOIN user_cohort uc ON s.origin_from_address = uc.origin_from_address
    INNER JOIN ethereum.uniswapv3.ez_pools p ON s.contract_address = p.pool_address
  WHERE
      platform = 'uniswap-v3'
  GROUP BY
    s.origin_from_address,
    cohort_ID
),

-- Cohort Size
cohort_size AS (
  SELECT
    uc.cohort_month,
    COUNT(DISTINCT uc.origin_from_address) as total_users,
    SUM(AMOUNT_IN_USD) as vol_usd,
    SUM(fee_percent * amount_in_usd) AS fee_usd,
    COUNT(*) as swap_count
  FROM
    user_cohort uc 
    LEFT JOIN ethereum.core.ez_dex_swaps s ON (s.origin_from_address = uc.origin_from_address
          AND DATE_TRUNC('month',block_timestamp) = uc.cohort_month)
    INNER JOIN ethereum.uniswapv3.ez_pools p ON s.contract_address = p.pool_address
  WHERE
      platform = 'uniswap-v3'
  GROUP BY
    uc.cohort_month
  ORDER BY
    uc.cohort_month

),

retention_table AS (
  SELECT
    c.cohort_month,
    f.cohort_ID,
    COUNT(*) as user_cohort,
    SUM(vol_usd) AS vol_usd_cohort,
    SUM(fee_usd) AS fee_usd_cohort,
    SUM(swap_count) AS swap_count_cohort
    --AVG(gas_price) AS avg_gas_price_cohort
  
  FROM
    following_month f
    LEFT JOIN user_cohort c ON f.origin_from_address = c.origin_from_address
  GROUP BY
    c.cohort_month,
    f.cohort_ID
)

-- Final view
SELECT
  DATE(r.cohort_month) as month ,
  'month_' || r.cohort_ID AS cohort_ID,
  s.total_users,
  r.user_cohort,
  s.vol_usd,
  vol_usd_cohort,
  s.fee_usd,
  fee_usd_cohort,
  s.swap_count
  swap_count_cohort,
  (s.vol_usd/s.swap_count) AS avg_tx_value,
  (vol_usd_cohort/swap_count_cohort) AS avg_tx_value_cohort
  
FROM
  retention_table r
  LEFT JOIN cohort_size s ON r.cohort_month = s.cohort_month
ORDER BY
  r.cohort_month,
  r.cohort_ID
"""

#metric by user
users = """
WITH gas_price_per_user AS (
  SELECT
    ORIGIN_FROM_ADDRESS,
    AVG(gas_price) avg_gas_price
  FROM
    (
      SELECT
            swap.block_timestamp,
            swap.tx_hash,
            origin_from_address,
            tx.gas_price,
            ROW_NUMBER() OVER(PARTITION BY swap.tx_hash order by swap.block_timestamp) as row_id
            
          FROM
            ethereum.core.ez_dex_swaps swap
            LEFT JOIN ethereum.core.fact_transactions tx
              ON (swap.tx_hash = tx.tx_hash AND swap.block_timestamp = tx.block_timestamp)
            
        WHERE
              platform = 'uniswap-v3'
        ORDER BY
          swap.block_timestamp
    )
  WHERE
    row_id = 1
  GROUP BY
      ORIGIN_FROM_ADDRESS
),

swap AS (
  SELECT
    ORIGIN_FROM_ADDRESS,
    SUM(COALESCE(AMOUNT_IN_USD,0)) as vol_usd,
    SUM(fee_percent * amount_in_usd) AS fee_usd,
    COUNT(*) as swap_count

  FROM
    ethereum.core.ez_dex_swaps swap
    INNER JOIN ethereum.uniswapv3.ez_pools pool ON swap.contract_address = pool.pool_address
  WHERE
    platform = 'uniswap-v3'
  GROUP BY
    ORIGIN_FROM_ADDRESS
  HAVING
    SUM(COALESCE(AMOUNT_IN_USD,0)) > 0
)
SELECT
  total.ORIGIN_FROM_ADDRESS,
  swap_count,
  vol_usd,
  (vol_usd/swap_count) AS avg_tx_value,
  fee_usd as revenue_usd,
  gas.avg_gas_price
FROM
  swap total
  INNER JOIN gas_price_per_user gas ON total.ORIGIN_FROM_ADDRESS = gas.ORIGIN_FROM_ADDRESS

ORDER BY
 vol_usd DESC
"""