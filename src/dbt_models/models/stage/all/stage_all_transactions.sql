-- Use the `ref` function to select from other models
{{ config(materialized='table') }}

WITH ing AS (
    SELECT
         cast(NULL as STRING) AS Prodict,
        * EXCEPT(Counterparty, Code, Debit_credit, Balance, Account)
    FROM {{ ref('stage_ing') }}
    WHERE
        NOT (REGEXP_CONTAINS(`Description`, r'Revolut') OR REGEXP_CONTAINS(Counterparty, r'REVO'))
),

revolut AS (
    SELECT
        cast(`Started Date` AS TIMESTAMP) AS Date,
        cast(NULL as STRING) AS Notifications,
        cast(NULL as STRING) AS Tag,
        * EXCEPT(`Started Date`, `Completed Date`, Fee, Currency, State, rownum, Balance)
    FROM {{ ref('stage_revolut') }}
    WHERE
        NOT (Type = 'TOPUP' AND (Description = 'OBA topup from Hr G Pavlidis' OR Description = 'IDEAL Top-Up'))
      AND Currency = 'EUR'
)

SELECT *
FROM ing
UNION ALL
SELECT *
FROM revolut
