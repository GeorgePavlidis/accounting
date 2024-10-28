{{ config(materialized='table') }}

WITH original AS (
    SELECT *,
        DATE_TRUNC(DATE_SUB(DATE(Date), INTERVAL 24 DAY), MONTH) AS custom_month_start
    FROM {{ ref('stage_all_transactions') }}
),

-- Filter positive and specific records only once here to avoid repetition
filtered_transactions AS (
    SELECT
        custom_month_start,
        Amount,
        Notifications,
        Description,
        CASE WHEN Amount > 0 AND NOT REGEXP_CONTAINS(Notifications, 'Oranje spaarrekening') THEN Amount ELSE 0 END AS income_amount,
        CASE WHEN REGEXP_CONTAINS(Notifications, 'Oranje spaarrekening') THEN -Amount ELSE 0 END AS savings_amount,
        CASE WHEN REGEXP_CONTAINS(Description, 'Hr I Trantas') THEN -Amount ELSE 0 END AS roomate_amount,
        CASE WHEN REGEXP_CONTAINS(Description, 'CBRE DRES Custodian I BV inzCBRE DRES II Actys R') THEN Amount ELSE 0 END AS rent_amount,
        CASE WHEN REGEXP_CONTAINS(Description, 'Vattenfall Klantenservice N.V.') THEN Amount ELSE 0 END AS electricity_and_gas_amount,
        CASE WHEN REGEXP_CONTAINS(Description, 'Waternet') THEN Amount ELSE 0 END AS water_amount,
        CASE WHEN REGEXP_CONTAINS(Notifications, 'Odido Internet + TV') THEN Amount ELSE 0 END AS internet_amount,
        CASE WHEN Description='Gemeente Amsterdam Belastingen' THEN Amount ELSE 0 END AS Municipality_Tax_amount,
        CASE WHEN REGEXP_CONTAINS(Notifications, r'(?i)Unive') THEN Amount ELSE 0 END AS insurance_amount,
        CASE WHEN REGEXP_CONTAINS(Notifications, 'ODIDO NETHERLANDS B.V.') and REGEXP_CONTAINS(Description, 'Mob') THEN Amount ELSE 0 END AS phone_amount,
        CASE WHEN REGEXP_CONTAINS(Description, r'(Dirk|Albert Heijn|Jumbo)') THEN Amount ELSE 0 END as supermarket_amount

    FROM original
),

-- Aggregate values by custom_month_start
aggregated AS (
    SELECT
        custom_month_start,
        SUM(income_amount) AS Income,
        SUM(savings_amount) AS Savings,
        SUM(roomate_amount) AS Roomate,
        SUM(rent_amount) AS Rent,
        SUM(electricity_and_gas_amount) AS Electricity_and_Gas,
        SUM(water_amount) AS Water,
        SUM(internet_amount) AS Internet,
        SUM(Municipality_Tax_amount) AS Municipality_Tax,
        SUM(CASE WHEN Amount > 0 AND REGEXP_CONTAINS(Description, 'Home Supplies') THEN Amount ELSE 0 END) AS Home_Stuff,
        SUM(CASE WHEN Amount > 0 AND REGEXP_CONTAINS(Description, 'Coffee Purchase') THEN Amount ELSE 0 END) AS Coffee,
        SUM(insurance_amount) AS Insurance,
        SUM(phone_amount) AS Phone,
        SUM(CASE WHEN Amount > 0 AND REGEXP_CONTAINS(Description, 'Car Expenses') THEN Amount ELSE 0 END) AS Car,
        SUM(CASE WHEN Amount > 0 AND REGEXP_CONTAINS(Description, 'Clothing') THEN Amount ELSE 0 END) AS Clothes,
        SUM(CASE WHEN Amount > 0 AND REGEXP_CONTAINS(Description, 'New Purchases') THEN Amount ELSE 0 END) AS New_Stuff,
        SUM(CASE WHEN Amount > 0 AND REGEXP_CONTAINS(Description, 'Miscellaneous') THEN Amount ELSE 0 END) AS Other,
        SUM(supermarket_amount) AS sumermarket,
        SUM(Amount) as Balance

    FROM filtered_transactions
    GROUP BY custom_month_start
)

SELECT *
FROM aggregated
ORDER BY custom_month_start DESC
