-- Use the `ref` function to select from other models
{{ config(materialized='table') }}

WITH original AS (
        SELECT *,
             DATE_TRUNC(
                DATE_SUB(DATE(Date), INTERVAL 24 DAY), 
                MONTH
            ) AS custom_month_start,
        FROM {{ ref('stage_all_transactions') }}
{#                  WHERE#}
{#    Date >= TIMESTAMP ('2024-09-25') AND Date#}
{#   < TIMESTAMP ('2024-10-24')#}
),
Income AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Income
    FROM
        original
    WHERE Amount > 0 and not regexp_contains(Notifications, 'Oranje spaarrekening')
    GROUP BY custom_month_start
),
Savings AS (
    SELECT
        custom_month_start,
        SUM(Amount)*-1 AS Savings
    FROM
        original
    WHERE regexp_contains(Notifications, 'Oranje spaarrekening')
    GROUP BY custom_month_start
),
Roomate AS (
    SELECT
        custom_month_start,
        SUM(Amount)*-1 AS Roomate
    FROM
        original
    WHERE regexp_contains(Description, 'Hr I Trantas')
    GROUP BY custom_month_start
),
Rent AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Rent
    FROM
        original
    WHERE regexp_contains(Description, 'CBRE DRES Custodian I BV inzCBRE DRES II Actys R')
    GROUP BY custom_month_start
),
electricity_and_gas AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Electricity_and_Gas
    FROM
        original
    WHERE regexp_contains(Description, 'Vattenfall Klantenservice N.V.')
    GROUP BY custom_month_start
),
water AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Water
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
internet AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Internet
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
municipality_tax AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Municipality_Tax
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
home_stuff AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Home_Stuff
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
coffee AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Coffee
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
insurance AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Insurance
    FROM
        original
    WHERE regexp_contains(Notifications, r'(?i)Unive')
    GROUP BY custom_month_start
),
phone AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Phone
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
car AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Car
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
monthly_spending AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Monthly_Spending
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
clothes AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Clothes
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
new_stuff AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS New_Stuff
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
),
other AS (
    SELECT
        custom_month_start,
        SUM(Amount) AS Other
    FROM
        original
    WHERE Amount > 0
    GROUP BY custom_month_start
)
SELECT
    Income.custom_month_start,
    Income.Income,
    Savings.Savings,
    Roomate.Roomate,
    Rent.Rent,
    electricity_and_gas.Electricity_and_Gas,
    water.Water,
    internet.Internet,
    municipality_tax.Municipality_Tax,
    home_stuff.Home_Stuff,
    coffee.Coffee,
    insurance.Insurance,
    phone.Phone,
    car.Car,
    monthly_spending.Monthly_Spending,
    clothes.Clothes,
    new_stuff.New_Stuff,
    other.Other

FROM Income
LEFT JOIN Savings ON TRUE
LEFT JOIN Roomate ON TRUE
LEFT JOIN Rent ON TRUE
LEFT JOIN electricity_and_gas ON TRUE
LEFT JOIN water ON TRUE
LEFT JOIN internet ON TRUE
LEFT JOIN municipality_tax ON TRUE
LEFT JOIN home_stuff ON TRUE
LEFT JOIN coffee ON TRUE
LEFT JOIN insurance ON TRUE
LEFT JOIN phone ON TRUE
LEFT JOIN car ON TRUE
LEFT JOIN monthly_spending ON TRUE
LEFT JOIN clothes ON TRUE
LEFT JOIN new_stuff ON TRUE
LEFT JOIN other ON TRUE
ORDER BY custom_month_start DESC
