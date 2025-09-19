import pandas as pd
import time
from alpha_vantage.timeseries import TimeSeries
from etl.spark_utils import get_spark
from etl.config_loader import load_config

def ingest_bronze():
    config = load_config()
    spark = get_spark("BronzeIngest")

    bronze_path = config["paths"]["bronze"]
    tickers = config["tickers"]

    ts = TimeSeries(key=config["alpha_vantage_key"], output_format="pandas")

    all_data = []

    for t in tickers:
        try:
            data, meta = ts.get_daily(symbol=t, outputsize="full")
            data.reset_index(inplace=True)
            data["Ticker"] = t
            data.rename(
                columns={
                    "date": "Date",
                    "1. open": "Open",
                    "2. high": "High",
                    "3. low": "Low",
                    "4. close": "Close",
                    "5. volume": "Volume",
                },
                inplace=True,
            )
            all_data.append(data)
            print(f"✅ Pulled {t}, rows: {len(data)}")
            time.sleep(12)  # respect free tier API rate
        except Exception as e:
            print(f"⚠️ Failed to fetch {t}: {e}")

    if not all_data:
        print("⚠️ No data fetched.")
        return

    pdf = pd.concat(all_data, ignore_index=True)

    df = spark.createDataFrame(pdf)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("Ticker")
        .save(bronze_path)
    )
    print(f"✅ Bronze (Delta) layer written to {bronze_path}")

if __name__ == "__main__":
    ingest_bronze()

