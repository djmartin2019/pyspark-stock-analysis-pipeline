from etl.spark_utils import get_spark
from etl.config_loader import load_config

def query_silver():
    config = load_config()
    spark = get_spark("QuerySilver")

    silver_path = config["paths"]["silver"]

    df = spark.read.format("delta").load(silver_path)

    print("✅ Silver table schema:")
    df.printSchema()

    print("\n📊 Sample Rows:")
    df.show(10)

    print("\n📈 Row count:")
    print(df.count())

    print("\n📊 Distinct tickers:")
    df.select("Ticker").distinct().show()

    print("\n🔍 AAPL last 5 rows:")
    (
        df.filter(df["Ticker"] == "AAPL")
            .orderBy("Date", ascending=False)
            .show(10)
    )

if __name__ == "__main__":
    query_silver()
