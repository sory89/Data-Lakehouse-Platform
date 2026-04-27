{{ config(materialized='table') }}

-- Customer LTV model with RFM scoring
WITH rfm AS (
    SELECT
        customer_id,
        segment,
        total_spent,
        order_count,
        last_order_at,
        clv_score,
        NTILE(4) OVER (ORDER BY total_spent DESC)                AS monetary_quartile,
        NTILE(4) OVER (ORDER BY order_count DESC)                AS frequency_quartile,
        NTILE(4) OVER (ORDER BY last_order_at DESC NULLS LAST)   AS recency_quartile
    FROM gold.customer_segments
),

scored AS (
    SELECT
        *,
        (monetary_quartile + frequency_quartile + recency_quartile) AS rfm_score,
        CASE
            WHEN monetary_quartile = 4 AND frequency_quartile = 4 THEN 'champion'
            WHEN monetary_quartile >= 3 AND frequency_quartile >= 3 THEN 'loyal'
            WHEN recency_quartile = 4                               THEN 'new'
            WHEN recency_quartile <= 2 AND monetary_quartile >= 3  THEN 'at_risk'
            ELSE 'regular'
        END AS rfm_segment
    FROM rfm
)

SELECT
    customer_id,
    segment,
    rfm_segment,
    rfm_score,
    total_spent,
    order_count,
    ROUND(total_spent / NULLIF(order_count, 0), 2) AS avg_order_value,
    last_order_at,
    ROUND(clv_score, 4) AS clv_score,
    monetary_quartile,
    frequency_quartile,
    recency_quartile
FROM scored
ORDER BY clv_score DESC
