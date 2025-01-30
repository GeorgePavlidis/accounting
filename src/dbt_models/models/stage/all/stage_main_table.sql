{{ config(materialized='table') }}

-- Aggregate values by custom_month_start
WITH aggregated AS (
    SELECT
        custom_month_start,
        min(show_month) as months,
        SUM(income_amount) AS Income,
        SUM(savings_amount) AS Savings,
        SUM(roomate_amount) AS Roomate,
        SUM(rent_amount) AS Rent,
        SUM(electricity_and_gas_amount) AS Electricity_and_Gas,
        SUM(water_amount) AS Water,
        SUM(internet_amount) AS Internet,
        SUM(Municipality_Tax_amount) AS Municipality_Tax,
        SUM(home_stuff_amount) AS Home_Stuff,
        SUM(coffee_amount) AS Coffee,
        ROUND(SUM(insurance_amount),2) AS Insurance,
        SUM(phone_amount) AS Phone,
        SUM(car_amount) AS Car,
        SUM(clothes_amount) AS Clothes,
        SUM(new_stuff_amount) AS New_Stuff,
        SUM(other_amount) AS Other,
        SUM(supermarket_amount) AS sumermarket,
        SUM(CASE WHEN savings_amount = 0 THEN Amount ELSE 0 END) AS Balance,
        -- Calculate total_house and total_fixed explicitly

    FROM  {{ ref('stage_filtered_transactions') }}
    GROUP BY custom_month_start
),

-- Summing fixed and house-related expenses
totals_1 AS (
    SELECT
        *,
        Water + Municipality_Tax + Rent + Internet + Electricity_and_Gas + Coffee + Home_Stuff + Roomate AS total_house,
        Water + Municipality_Tax + Rent + Internet + Electricity_and_Gas + Coffee + Home_Stuff + Roomate
        + Insurance + phone + Car  AS total_fixed_cost

    FROM aggregated
),
totals AS (
    SELECT
        *,
        total_fixed_cost + total_house AS total_spendings
    FROM totals_1
)

SELECT *
FROM totals
ORDER BY custom_month_start DESC
