from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Per-table cleaning config:
#   date_cols    — columns to normalise to ISO DateType
#   drop_nulls   — columns where a NULL means the row must be dropped
#   tier_col     — column to standardise to lowercase (customers only)
#   flag_negative — column to inspect for negative values (orders only)
TABLE_CONFIG = {
    "orders": {
        "date_cols":     ["order_date"],
        "drop_nulls":    ["order_id", "customer_id"],
        "flag_negative": "total_amount",
    },
    "customers": {
        "date_cols":  ["signup_date"],
        "drop_nulls": ["customer_id"],
        "tier_col":   "customer_tier",
    },
    "order_items": {
        "date_cols":  [],
        "drop_nulls": ["item_id"],
    },
    "returns": {
        "date_cols":  ["return_date"],
        "drop_nulls": ["return_id"],
    },
}

def clean_table(df, table):
    config = TABLE_CONFIG[table]
    
    # drop duplicates
    clean_df = df.dropDuplicates()
    
    # standardise date formats (source mixes yyyy-MM-dd and dd/MM/yyyy)
    for col_name in config.get("date_cols", []):
        clean_df = clean_df.withColumn(
            col_name,
            F.coalesce(
                F.to_date(F.col(col_name), "yyyy-MM-dd"),
                F.to_date(F.col(col_name), "dd/MM/yyyy"),
            ),
        )
    
    # lowercase tier column
    tier_col = config.get("tier_col")
    if tier_col:
        clean_df = clean_df.withColumn(tier_col, F.lower(F.col(tier_col)))
        
    # drop rows missing required keys
    clean_df = clean_df.dropna(subset=config.get("drop_nulls", []))
    
    # flag negative amounts
    flag_col = config.get("flag_negative")
    if flag_col:
        clean_df = clean_df.withColumn(
            "negative_amount_flag",
            F.when(F.col(flag_col) < 0, True).otherwise(False)
        )
    return clean_df

def enrich_orders(orders_clean,customers_clean,order_items_clean):
    # Enrich orders with customer and item details
    enriched = (orders_clean
        .join(customers_clean, "customer_id", "inner")
        .join(order_items_clean, "order_id", "left")
    )
    
    # Derived column
    enriched = enriched.withColumn(
        "net_amount",
        F.col("total_amount") * (1 - F.col("discount_pct") / 100)
    )

    # Partition columns derived from order_date
    enriched = (enriched
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
    )
    
    # orphaned items
    orphaned_items = order_items_clean.join(
        orders_clean.select("order_id"), "order_id", "left_anti"
    )

    return enriched, orphaned_items


def rank_customers(enriched):
    # Rank customers by total net amount spent
    customer_spend = (enriched
        .groupBy("customer_id", "country")
        .agg(F.sum("net_amount").alias("total_spent"))
    )
    
    window_spec = Window.partitionBy("country").orderBy(F.desc("total_spent"))
    ranked_customers = customer_spend.withColumn(
        "rank", F.row_number().over(window_spec)
    )
    
    return ranked_customers


def rolling_order_count(enriched):
    # Calculate rolling 7-day order count per customer
    orders_by_date = (enriched
        .groupBy("customer_id", "order_date")
        .agg(F.countDistinct("order_id").alias("daily_orders"))
        .orderBy("order_date")
    )
    
    window_spec = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(-6, 0)
    orders_by_date = orders_by_date.withColumn(
        "rolling_7d_orders", F.sum("daily_orders").over(window_spec)
    )
    
    return orders_by_date

def category_revenue_share(enriched):
    # Calculate revenue share by product category
    category_revenue = (enriched
        .groupBy("category")
        .agg(F.sum("net_amount").alias("total_revenue"))
        .withColumn("revenue_share", F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy()))
    )

    return category_revenue

def returns_analysis(enriched, returns_clean):
    """Join the returns table to your enriched orders. Compute the return rate (returns / orders) per category and per
customer_tier. Identify the top 10 customers by total refund amount. Add a boolean column
refund_exceeds_order to flag returns where refund_amount > net_amount."""
    
    orders_deduped = (
        enriched
        .select(
            "order_id", "customer_id", "country",
            "customer_tier", "category", "net_amount",
        )
        .dropDuplicates(["order_id"])
    )
    
    returns_enriched = returns_clean.join(
        orders_deduped,
        on="order_id",
        how="left",  
    ).withColumn(
        "refund_exceeds_order",
        F.col("refund_amount") > F.col("net_amount"),
    )    
    
    
    total_orders_by_category = orders_deduped.groupBy("category").agg(F.countDistinct("order_id").alias("total_orders"))
    returns_by_category = returns_enriched.groupBy("category").agg(F.countDistinct("order_id").alias("total_returns"))
    
    total_orders_by_tier = orders_deduped.groupBy("customer_tier").agg(F.countDistinct("order_id").alias("total_orders"))
    returns_by_tier = returns_enriched.groupBy("customer_tier").agg(F.countDistinct("order_id").alias("total_returns"))
    
    top_refund_customers = (returns_enriched
        .groupBy("customer_id")
        .agg(F.sum("refund_amount").alias("total_refund"))
        .orderBy(F.desc("total_refund"))
        .limit(10)
    )
    return returns_enriched, returns_by_category, returns_by_tier, top_refund_customers