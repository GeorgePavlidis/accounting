-- Use the `ref` function to select from other models
{{ config(materialized='table') }}

WITH ing AS (
    SELECT
        Date,
        Bank,
        Amount,
        Description,
        Type,
        cast(NULL as STRING) AS Product,
        Notifications,
        Tag
    FROM {{ ref('stage_ing') }}
    WHERE
        NOT (REGEXP_CONTAINS(`Description`, r'Revolut') OR REGEXP_CONTAINS(Counterparty, r'REVO'))
),

revolut AS (
    SELECT
        `Started Date`  AS Date,
        Bank,
        Amount,
        Description,
        Type,
        Product,
        cast(NULL as STRING) AS Notifications,
        CAST('' as STRING) AS Tag
    FROM {{ ref('stage_revolut') }}
    WHERE
        NOT (Type = 'TOPUP' AND (Description = 'OBA topup from Hr G Pavlidis' OR Description = 'IDEAL Top-Up'))
      AND Currency = 'EUR'
),
original AS (
    SELECT *
    FROM ing
    UNION ALL
    SELECT *
    FROM revolut
    order by Date desc
),

orginal_with_tags AS (
    SELECT * except(Tag),
        CASE
            WHEN REGEXP_CONTAINS(Description, r"(Dirk|Albert Heijn|Jumbo)") THEN
                IF(Tag IS NOT NULL AND Tag != '', CONCAT(Tag, ', supermarket'), 'supermarket')
            ELSE
                Tag
        END AS Tag
    FROM original
)
SELECT *
FROM orginal_with_tags
