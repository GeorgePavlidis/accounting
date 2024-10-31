{{ config(materialized='table') }}

WITH original AS (
    SELECT *,
        DATE_TRUNC(DATE_SUB(DATE(Date), INTERVAL 24 DAY), MONTH) AS custom_month_start
    FROM {{ ref('stage_all_transactions') }}
)
{#        DATE_TRUNC(DATE_ADD(DATE_SUB(DATE(Date), INTERVAL 24 DAY), INTERVAL 1 MONTH), MONTH) AS custom_month_start#}
-- Filter positive and specific records only once here to avoid repetition
SELECT
    custom_month_start,
    Date,
    Amount,
    Notifications,
    Description,
    CASE
        WHEN Amount > 0 AND NOT REGEXP_CONTAINS(Notifications, 'Oranje spaarrekening') THEN Amount ELSE 0
    END AS income_amount,
    CASE
        WHEN REGEXP_CONTAINS(Notifications, 'Oranje spaarrekening') THEN -Amount
        ELSE 0
    END AS savings_amount,
    CASE
        WHEN REGEXP_CONTAINS(Description, 'Hr I Trantas') THEN Amount
        ELSE 0
    END AS roomate_amount,
    CASE
        WHEN REGEXP_CONTAINS(Description, 'CBRE DRES Custodian') THEN Amount
        ELSE 0
    END AS rent_amount,
    CASE
        WHEN REGEXP_CONTAINS(Description, 'Vattenfall Klantenservice N.V.') THEN Amount
        ELSE 0
    END AS electricity_and_gas_amount,
    CASE
        WHEN REGEXP_CONTAINS(Description, 'Waternet') THEN Amount
        ELSE 0
    END AS water_amount,
    CASE
        WHEN REGEXP_CONTAINS(Notifications, 'Odido Internet') THEN Amount
        ELSE 0
    END AS internet_amount,
    CASE
        WHEN Description='Gemeente Amsterdam Belastingen' THEN Amount
        ELSE 0
    END AS Municipality_Tax_amount,
    CASE
        WHEN REGEXP_CONTAINS(Notifications, r'(?i)Unive') THEN Amount
        ELSE 0
    END AS insurance_amount,
    CASE
        WHEN REGEXP_CONTAINS(Description, 'ODIDO NETHERLANDS B.V.') and REGEXP_CONTAINS(Notifications, 'Mob')
        THEN Amount
        ELSE 0
    END AS phone_amount,
    CASE
        WHEN REGEXP_CONTAINS(Description, r'(Dirk|Albert Heijn|Jumbo|Action)') THEN Amount
        ELSE 0
    END as supermarket_amount,

        -- Additional categories for variable expenses
    CASE WHEN REGEXP_CONTAINS(Description, 'Home Supplies') THEN Amount ELSE 0 END AS home_stuff_amount,
    CASE WHEN REGEXP_CONTAINS(Description, 'Nespresso') THEN Amount ELSE 0 END AS coffee_amount,
    CASE WHEN REGEXP_CONTAINS(Description, 'Car Expenses') THEN Amount ELSE 0 END AS car_amount,
    CASE WHEN REGEXP_CONTAINS(Description, 'Clothing') THEN Amount ELSE 0 END AS clothes_amount,
    CASE WHEN REGEXP_CONTAINS(Description, 'New Purchases') THEN Amount ELSE 0 END AS new_stuff_amount,
    CASE WHEN REGEXP_CONTAINS(Description, 'Miscellaneous') THEN Amount ELSE 0 END AS other_amount

FROM original
ORDER BY custom_month_start DESC
