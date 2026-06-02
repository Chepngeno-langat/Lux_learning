import argparse
from pyspark.sql import SparkSession

from pipeline.ingestion import load_all
from pipeline.transform import (clean_table, enrich_orders, rank_customers, rolling_order_count, category_revenue_share, returns_analysis)
from pipeline.load import write_enriched_parquet, write_summaries, write_rejected, write_orphaned_items, write_returns_enriched

def main(args):
    spark = SparkSession.builder.appName("EcommerceDataPipeline").getOrCreate()
    
    # Ingest
    print("\n=== Ingestion ===")
    data = load_all(spark, args.data_dir)
    
    # Clean
    print("\n=== Cleaning ===")
    orders_clean = clean_table(data["orders"], "orders")
    customers_clean = clean_table(data["customers"], "customers")
    order_items_clean = clean_table(data["order_items"], "order_items")
    returns_clean = clean_table(data["returns"], "returns")
    
    # Transform
    print("\n=== Enrichment ===")
    enriched, orphaned_items = enrich_orders(orders_clean, customers_clean, order_items_clean)
    print("\n=== Aggregation ===")
    customer_spend_rank = rank_customers(enriched)
    rolling_orders = rolling_order_count(enriched)
    category_share = category_revenue_share(enriched)
    returns_enriched, return_by_category, return_by_tier, top_refund_customers = returns_analysis(enriched, returns_clean)

    # Write
    print("\n=== Writing Results ===")
    write_enriched_parquet(enriched, f"{args.output_dir}/enriched")
    write_orphaned_items(orphaned_items, f"{args.output_dir}/orphaned_items")
    write_returns_enriched(returns_enriched, f"{args.output_dir}/returns_enriched")
    write_summaries(
        customer_spend_rank=customer_spend_rank,
        rolling_orders=rolling_orders,
        category_revenue_share=category_share,
        return_by_category=return_by_category,
        return_by_tier=return_by_tier,
        top_refund_customers=top_refund_customers,
        output_dir=args.output_dir,
    )
    write_rejected(
        {
            "rejected_orders":      data["rejected_orders"],
            "rejected_order_items": data["rejected_order_items"],
        },
        args.output_dir,
    )
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce Data Pipeline")
    parser.add_argument("--data_dir", type=str, default="data", help="Directory containing input CSV files")
    parser.add_argument("--output_dir", type=str, default="output", help="Directory to write output files")
    args = parser.parse_args()
    
    main(args)