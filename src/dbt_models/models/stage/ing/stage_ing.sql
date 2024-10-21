
-- Use the `ref` function to select from other models
{{ config(materialized='table') }}

WITH ranked_ing AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY `Resulting balance`, `Amount _EUR_`, Notifications ORDER BY `Resulting balance`, `Amount _EUR_`, Notifications) AS rownum
  FROM {{ source('landing', 'ing') }}
)

SELECT
    CAST(PARSE_TIMESTAMP('%Y%m%d', CAST(Date AS STRING)) AS TIMESTAMP) AS Date,
    CASE
        WHEN `Debit_credit` = 'Debit' THEN CAST(`Amount _EUR_` AS FLOAT64) * -1 / 100
        ELSE (CAST(`Amount _EUR_` AS FLOAT64) / 100)
    END AS Amount,
    CAST(`Resulting balance` AS FLOAT64) / 100 AS Balance,
    * except (Date, `Amount _EUR_`, `Resulting balance`)
FROM ranked_ing
WHERE rownum = 1