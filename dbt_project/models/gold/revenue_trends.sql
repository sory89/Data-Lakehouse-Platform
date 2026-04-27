{{ config(materialized='table') }}

-- Revenue trends: 7-day rolling average + MoM growth
WITH daily AS (
    SELECT
        date,
        total_orders,
        total_revenue,
        avg_order,
        unique_customers,
        top_category
    FROM gold.daily_sales
),

rolling AS (
    SELECT
        date,
        total_orders,
        total_revenue,
        avg_order,
        unique_customers,
        top_category,
        ROUND(AVG(total_revenue) OVER (
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS revenue_7d_avg,
        ROUND(SUM(total_revenue) OVER (
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS revenue_7d_sum,
        LAG(total_revenue, 30) OVER (ORDER BY date) AS revenue_30d_ago
    FROM daily
)

SELECT
    date,
    total_orders,
    total_revenue,
    avg_order,
    unique_customers,
    top_category,
    revenue_7d_avg,
    revenue_7d_sum,
    CASE
        WHEN revenue_30d_ago > 0
        THEN ROUND((total_revenue - revenue_30d_ago) / revenue_30d_ago * 100, 2)
        ELSE NULL
    END AS mom_growth_pct
FROM rolling
ORDER BY date DESC
