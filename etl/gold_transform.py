from etl.spark_utils import get_spark
from etl.config_loader import load_config
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def transform_gold():
    try:
        config = load_config()
        spark = get_spark("GoldTransform")

        silver_path = config["paths"]["silver"]
        gold_path = config["paths"]["gold"]

        try:
            df = spark.read.format("delta").load(silver_path)
        except AnalysisException:
            print("❌ No Silver table found at {silver_path}. Run Silver ingestion first")
            return

        row_count = df.count()
        if row_count == 0:
            print(f"❌ Silver table at {silver_path} is empty. Nothing to transform.")
            return

        # Define a per-ticker window ordered by date
        ticker_window = Window.partitionBy("ticker").orderBy("date")
        rolling20 = ticker_window.rowsBetween(-19, 0)
        rolling50 = ticker_window.rowsBetween(-49, 0)

        # previous close
        df = df.withColumn("prev_close", F.lag("close").over(ticker_window))

        # daily return (pct)
        df = df.withColumn("ret", (F.col("close") / F.col("prev_close")) - F.lit(1.0))

        # cumulative return as % (starts at 0.0)
        df = df.withColumn("cum_ret",
                           F.exp(F.sum(F.log1p("ret")).over(ticker_window)) - F.lit(1.0))

        # moving averages
        df = df.withColumn("sma20", F.avg("close").over(rolling20)) \
               .withColumn("sma50", F.avg("close").over(rolling50))

        df = df.withColumn(
            "cum_max",
            F.max("cum_ret").over(ticker_window)
        ).withColumn(
            "drawdown",
            F.col("cum_ret") - F.col("cum_max")
        )

        df = df.withColumn(
            "sharpe20",
            (F.avg("ret").over(rolling20)) / (F.stddev("ret").over(rolling20))
        )

        # count of non-null returns in window (Spark's count ignores nulls)
        cnt20 = F.count("ret").over(rolling20)
        cnt50 = F.count("ret").over(rolling50)

        # rolling volatility (annualized), only when full window is present
        ann = F.sqrt(F.lit(252.0))
        vol20_raw = F.stddev("ret").over(rolling20)
        vol50_raw = F.stddev("ret").over(rolling50)

        df = df.withColumn("vol20", F.when(cnt20 >= 20, ann * vol20_raw)) \
               .withColumn("vol50", F.when(cnt50 >= 50, ann * vol50_raw))

        # keep rows; we'll drop only where we truly need to
        df = df.dropna(subset=["ret", "sma20", "sma50"])

        # Write Gold layer
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ticker")
            .save(gold_path)
        )

        print(f"✅ Gold table written to {gold_path}")
        print(f"📊 Row count: {df.count()}, Columns: {df.columns}")

    except Exception as e:
        print(f"🔥 Gold transform failed: {e}")

if __name__ == "__main__":
    transform_gold()
