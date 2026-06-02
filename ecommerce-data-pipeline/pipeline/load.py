"""
Write your final enriched dataset to Parquet, partitioned by order year and order month. Write aggregated
summary tables to CSV. All writes must use mode(‘overwrite’) so the pipeline is idempotent."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def write_enriched_parquet(enriched, output_path):
    enriched.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(output_path)
    
    print(f"[LOAD] Enriched data written to {output_path}")
    
def write_orphaned_items(orphaned_items, output_path):
    orphaned_items.write.mode("overwrite").parquet(output_path)
    
    print(f"[LOAD] Orphaned items written to {output_path}")


def write_returns_enriched(returns_enriched, output_path):
    returns_enriched.write.mode("overwrite").parquet(output_path)
    
    print(f"[LOAD] Enriched returns written to {output_path}")
    

# CSV writers (aggregated summary tables)
def write_csv(df, output_path, name=""):
    (df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"[LOAD] Summary table '{name}' written to {output_path}")
    
def write_summaries(
    customer_spend_rank:     DataFrame,
    rolling_orders:          DataFrame,
    category_revenue_share:  DataFrame,
    return_by_category:      DataFrame,
    return_by_tier:          DataFrame,
    top_refund_customers:    DataFrame,
    output_dir: str,
) -> None:
    """Write all aggregated summary tables to CSV."""
    write_csv(customer_spend_rank,    f"{output_dir}/summary_customer_spend_rank",   "customer_spend_rank")
    write_csv(rolling_orders,         f"{output_dir}/summary_rolling_order_count",   "rolling_order_count")
    write_csv(category_revenue_share, f"{output_dir}/summary_category_revenue_share","category_revenue_share")
    write_csv(return_by_category,     f"{output_dir}/summary_return_by_category",    "return_by_category")
    write_csv(return_by_tier,         f"{output_dir}/summary_return_by_tier",        "return_by_tier")
    write_csv(top_refund_customers,   f"{output_dir}/summary_top_refund_customers",  "top_refund_customers")
    

# Rejected rows
def write_rejected(rejected_dfs: dict, output_dir: str) -> None:
    """Persist rejected rows for audit purposes."""
    for name, df in rejected_dfs.items():
        if df.count() > 0:
            write_csv(df, f"{output_dir}/rejected/{name}", f"rejected/{name}")   
