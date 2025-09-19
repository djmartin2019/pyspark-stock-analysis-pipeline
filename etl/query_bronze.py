from etl.spark_utils import get_spark
from etl.config_loader import load_config

def query_bronze():
    config = load_config()
    spark = get_spark("QueryBronze")

    bronze_path = config["paths"]["bronze"]

    df = spark.read.format("delta").load(bronze_path)

    print("✅ Bronze table schema:")
    df.printSchema()

    print("\n📊 Sample Rows:")
    df.show(10)

    print("\n📈 Row count:")
    print(df.count())

    print("\n📊 Distinct tickers:")
    df.select("Ticker").distinct().show()

    print("\n🥇 AAPL first 10 rows:")
    (
        df.filter(df["Ticker"] == "AAPL")
            .orderBy("Date", ascending=True)
            .show(10)
    )

    print("\n🔍 AAPL last 10 rows:")
    (
        df.filter(df["Ticker"] == "AAPL")
            .orderBy("Date", ascending=False)
            .show(10)
    )

    print("\n🥇 GOOG first 10 rows:")
    (
        df.filter(df["Ticker"] == "GOOG")
            .orderBy("Date", ascending=True)
            .show(10)
    )

    print("\n🔍 GOOG last 10 rows:")
    (
        df.filter(df["Ticker"] == "GOOG")
            .orderBy("Date", ascending=False)
            .show(10)
    )

if __name__ == "__main__":
    query_bronze()
