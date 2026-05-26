from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F

# spark = SparkSession.builder.appName("EcommercePipeline").getorCreate()

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True)
])

order_items_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True)
])

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("signup_date", StringType(), True)
])

returns_schema = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("reason", StringType(), True)
])

# Load data
def load_data(file_path, schema):
    return (spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(file_path)
    )

# Rejected rows
def split_rejected_rows(df, key_col):
    rejected = df.filter(F.col(key_col).isNull())
    clean    = df.filter(F.col(key_col).isNotNull())
    return clean, rejected
    
    
# Loaders
def load_orders(spark, path):
    raw = load_data(spark, path, orders_schema)
    return split_rejected_rows(raw, "order_id")
 
 
def load_order_items(spark, path):
    raw = load_data(spark, path, order_items_schema)
    return split_rejected_rows(raw, "item_id")
 
 
def load_customers(spark, path):
    raw = load_data(spark, path, customers_schema)
    return split_rejected_rows(raw, "customer_id")
 
 
def load_returns(spark, path):
    raw = load_data(spark, path, returns_schema)
    return split_rejected_rows(raw, "return_id")
    
def load_all(spark: SparkSession, data_dir: str):
    orders,       rej_orders       = load_orders(spark, f"/output/orders.csv")
    order_items,  rej_order_items  = load_order_items(spark, f"/output/order_items.csv")
    customers,    rej_customers    = load_customers(spark, f"/output/customers.csv")
    returns,      rej_returns      = load_returns(spark, f"/output/returns.csv")
 
    # Log rejection counts
    for name, df in [
        ("orders",       rej_orders),
        ("order_items",  rej_order_items),
        ("customers",    rej_customers),
        ("returns",      rej_returns),
    ]:
        n = df.count()
        if n > 0:
            print(f"[INGEST] ⚠  {n} rejected rows in {name}")
 
    return {
        "orders":               orders,
        "order_items":          order_items,
        "customers":            customers,
        "returns":              returns,
        "rejected_orders":      rej_orders,
        "rejected_order_items": rej_order_items,
        "rejected_customers":   rej_customers,
        "rejected_returns":     rej_returns,
    }