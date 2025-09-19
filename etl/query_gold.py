from etl.spark_utils import get_spark
from etl.config_loader import load_config

def query_gold():
    config = load_config()
    spark = get_spark("QueryGold")

    gold_path = config["paths"]["gold"]

    df = spark.read.format("delta").load(gold_path)

    print("✅ Gold table schema:")
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
    query_gold()
