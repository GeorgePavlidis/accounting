
-- Use the `ref` function to select from other models
{{ config(materialized='table') }}

WITH ranked_revolut AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(Amount AS STRING), CAST(Balance AS STRING), `Started Date`, Description
      ORDER BY `Started Date`, Description,  CAST(Amount AS STRING),  CAST(Balance AS STRING)
    ) AS rownum
  FROM {{ source('landing', 'revolut') }}
)

SELECT
    *,
    'Revolut' as Bank
FROM ranked_revolut
WHERE rownum = 1