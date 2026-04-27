{{ config(materialized='table') }}

-- Top products ranked by revenue with category context
WITH ranked AS (
    SELECT
        product_id,
        product_name,
        category,
        period,
        units_sold,
        revenue,
        avg_price,
        return_rate,
        RANK() OVER (PARTITION BY period ORDER BY revenue DESC) AS revenue_rank,
        RANK() OVER (PARTITION BY period ORDER BY units_sold DESC) AS volume_rank,
        ROUND(
            revenue / NULLIF(SUM(revenue) OVER (PARTITION BY period), 0) * 100, 2
        ) AS revenue_share_pct
    FROM gold.product_performance
)

SELECT
    product_id,
    product_name,
    category,
    period,
    units_sold,
    revenue,
    avg_price,
    ROUND(return_rate * 100, 2) AS return_rate_pct,
    revenue_rank,
    volume_rank,
    revenue_share_pct
FROM ranked
ORDER BY period DESC, revenue_rank ASC
