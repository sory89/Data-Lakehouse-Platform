"""
Customer Feature Definitions — Netflix-style Feature Store
Features used for:
- Churn prediction
- LTV scoring  
- Product recommendations
- Fraud detection
"""

from datetime import timedelta
from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast.types import Float32, Int64, String

# ── Entities ───────────────────────────────────────────────────────────────────
customer = Entity(
    name="customer_id",
    value_type=ValueType.STRING,
    description="Unique customer identifier",
)

product = Entity(
    name="product_id", 
    value_type=ValueType.STRING,
    description="Unique product identifier",
)

# ── Sources ────────────────────────────────────────────────────────────────────
customer_stats_source = FileSource(
    path="s3://silver/features/customer_stats/",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

product_stats_source = FileSource(
    path="s3://silver/features/product_stats/",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# ── Feature Views ──────────────────────────────────────────────────────────────
customer_features = FeatureView(
    name="customer_features",
    entities=["customer_id"],
    ttl=timedelta(days=7),
    features=[
        Feature(name="order_count_7d",    dtype=Int64),
        Feature(name="order_count_30d",   dtype=Int64),
        Feature(name="order_count_90d",   dtype=Int64),
        Feature(name="total_spent_30d",   dtype=Float32),
        Feature(name="avg_basket_size",   dtype=Float32),
        Feature(name="days_since_last",   dtype=Int64),
        Feature(name="return_rate",       dtype=Float32),
        Feature(name="clv_score",         dtype=Float32),
        Feature(name="segment",           dtype=String),
        Feature(name="churn_probability", dtype=Float32),
        Feature(name="preferred_category",dtype=String),
    ],
    online=True,
    source=customer_stats_source,
    tags={"team": "ml", "use_case": "churn,ltv,recommendations"},
)

product_features = FeatureView(
    name="product_features",
    entities=["product_id"],
    ttl=timedelta(days=1),
    features=[
        Feature(name="units_sold_7d",     dtype=Int64),
        Feature(name="units_sold_30d",    dtype=Int64),
        Feature(name="revenue_30d",       dtype=Float32),
        Feature(name="avg_rating",        dtype=Float32),
        Feature(name="return_rate",       dtype=Float32),
        Feature(name="stock_level",       dtype=Int64),
        Feature(name="price_tier",        dtype=String),
        Feature(name="category",          dtype=String),
    ],
    online=True,
    source=product_stats_source,
    tags={"team": "ml", "use_case": "recommendations,pricing"},
)
