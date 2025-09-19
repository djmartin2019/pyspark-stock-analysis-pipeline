from etl.spark_utils import get_spark
from etl.config_loader import load_config
from pyspark.sql.functions import to_date, col

def transform_silver():
    try:
        config = load_config()
        spark = get_spark("SilverTransform")

        bronze_path = config["paths"]["bronze"]
        silver_path = config["paths"]["silver"]

        try:
            df = spark.read.format("delta").load(bronze_path)
        except AnalysisException:
            print(f"❌ No Bronze table found at {bronze_path}. Run Bronze ingestion first")
            return

        if df.count() == 0:
            print(f"❌ Bronze table at {bronze_path} is empty. Nothing to transform.")
            return

        # Schema cleanup
        df = (
                df.withColumn("Date", to_date(col("Date")))
                    .withColumn("Volume", col("Volume").cast("long"))
                    .withColumn("Open", col("Open").cast("double"))
                    .withColumn("High", col("High").cast("double"))
                    .withColumn("Low", col("Low").cast("double"))
                    .withColumn("Close", col("Close").cast("double"))
            )

        # Normalize column names
        rename_map = {
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Ticker": "ticker"
        }

        for old, new in rename_map.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

        # Drop rows with missing close price
        df = df.dropna(subset=["close"])

        if df.count() == 0:
            print("❌ After cleaning, no rows remain. Skipping Silver write.")
            return

        # Write Silver layer
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ticker")
            .save(silver_path)
        )

        print(f"✅ Silver table written to {silver_path}")
        print(f"📊 Row count: {df.count()}, Columns: {df.columns}")

    except Exception as e:
        print(f"🔥 Silver transform failed: {e}")

if __name__ == "__main__":
    transform_silver()
