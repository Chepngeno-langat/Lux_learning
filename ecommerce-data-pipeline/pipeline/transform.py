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
    
    # standardise date formats
    for col_name in config.get("date_cols", []):
        clean_df = clean_df.withColumn(col_name, F.to_date(F.col(col_name), "yyyy-MM-dd"))
    
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