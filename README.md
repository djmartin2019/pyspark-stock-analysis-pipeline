# 📊 Alpha Vantage Stock Analysis Dashboard

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://python.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange.svg)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-green.svg)](https://delta.io)
[![Dash](https://img.shields.io/badge/Dash-2.17.0-purple.svg)](https://dash.plotly.com)
[![Plotly](https://img.shields.io/badge/Plotly-5.23.0-red.svg)](https://plotly.com)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Alpha Vantage](https://img.shields.io/badge/Data%20Source-Alpha%20Vantage-cyan.svg)](https://www.alphavantage.co)

A comprehensive stock market analysis platform built with Apache Spark, Delta Lake, and Dash. This project implements a modern data engineering pipeline following the medallion architecture pattern to ingest, process, and visualize stock market data from Alpha Vantage API.

## 🏗️ Architecture Overview

This project follows the **Medallion Architecture** pattern with three data layers:

- **🥉 Bronze Layer**: Raw data ingestion from Alpha Vantage API
- **🥈 Silver Layer**: Cleaned and standardized data with proper schema
- **🥇 Gold Layer**: Enriched data with technical indicators and analytics

### Technology Stack

- **Data Processing**: Apache Spark 3.5.1 with Delta Lake 3.2.0
- **Data Ingestion**: Alpha Vantage API with yfinance wrapper
- **Visualization**: Dash 2.17.0 with Plotly 5.23.0
- **Data Storage**: Parquet format with Delta Lake ACID transactions
- **Development**: Jupyter Lab for prototyping and analysis

## 🚀 Features

### Data Pipeline

- **Automated Data Ingestion**: Fetches historical stock data from Alpha Vantage API
- **Schema Evolution**: Handles data type conversions and column standardization
- **Technical Indicators**: Calculates moving averages, volatility, returns, and Sharpe ratios
- **ACID Transactions**: Delta Lake ensures data consistency and time travel capabilities

### Interactive Dashboard

- **Real-time Visualization**: Interactive charts with Plotly
- **Multi-ticker Support**: Compare performance across different stocks
- **Technical Analysis**: Candlestick charts with moving averages
- **Risk Metrics**: Volatility analysis and drawdown calculations
- **Performance Comparison**: Leaderboard showing top and bottom performers

## 📁 Project Structure

```
alpha-vantage-dashboard/
├── config/
│   └── config.json              # Configuration settings
├── dashboard/
│   ├── app.py                   # Main Dash application
│   └── assets/                  # CSS styling files
├── data/
│   ├── bronze/                  # Raw ingested data
│   ├── silver/                  # Cleaned and standardized data
│   └── gold/                    # Enriched analytics data
├── etl/
│   ├── bronze_ingest.py         # Data ingestion from Alpha Vantage
│   ├── silver_transform.py      # Data cleaning and standardization
│   ├── gold_transform.py        # Technical indicators and analytics
│   ├── spark_utils.py           # Spark session configuration
│   └── config_loader.py         # Configuration management
├── notebooks/
│   └── prototyping.ipynb       # Jupyter notebook for analysis
├── requirements.txt             # Python dependencies
└── Dockerfile                   # Container configuration
```

## 🛠️ Installation & Setup

### Prerequisites

- Python 3.8+
- Java 8+ (required for Apache Spark)
- Alpha Vantage API key

### 1. Clone the Repository

```bash
git clone https://github.com/djmartin2019/pyspark-stock-analysis-pipeline.git
cd alpha-vantage-dashboard
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Alpha Vantage API

Update `config/config.json` with your Alpha Vantage API key:

```json
{
  "paths": {
    "bronze": "data/bronze/stock_prices/",
    "silver": "data/silver/stock_prices/",
    "gold": "data/gold/stock_prices/"
  },
  "tickers": ["AAPL", "GOOG"],
  "alpha_vantage_key": "YOUR_API_KEY_HERE"
}
```

### 4. Run the ETL Pipeline

Execute the data pipeline in sequence:

```bash
# Step 1: Ingest raw data
python etl/bronze_ingest.py

# Step 2: Clean and standardize data
python etl/silver_transform.py

# Step 3: Calculate technical indicators
python etl/gold_transform.py
```

### 5. Launch the Dashboard

```bash
python dashboard/app.py
```

The dashboard will be available at `http://localhost:8050`

## 📊 Data Pipeline Details

### Bronze Layer (Raw Data)

- Fetches daily OHLCV data from Alpha Vantage API
- Implements rate limiting (12-second delays) for free tier compliance
- Stores data in Delta Lake format with ticker partitioning
- Handles API errors gracefully with retry logic

### Silver Layer (Cleaned Data)

- Standardizes column names and data types
- Validates data quality and removes invalid records
- Converts date strings to proper date format
- Ensures consistent schema across all tickers

### Gold Layer (Analytics Data)

- **Technical Indicators**: 20-day and 50-day Simple Moving Averages
- **Returns Analysis**: Daily returns and cumulative returns
- **Risk Metrics**: Rolling volatility (20-day and 50-day)
- **Performance Metrics**: Sharpe ratio and maximum drawdown
- **Window Functions**: Efficient calculation using Spark SQL window functions

## 🎯 Dashboard Features

### Interactive Components

- **Ticker Selection**: Dropdown to switch between available stocks
- **Date Range Picker**: Filter data by custom date ranges
- **KPI Cards**: Key metrics display (last close, cumulative return, volatility)

### Visualization Types

- **Candlestick Charts**: OHLC price data with moving averages
- **Returns Analysis**: Daily and cumulative returns over time
- **Volatility Charts**: Rolling volatility indicators
- **Performance Leaderboard**: Top 5 vs Bottom 5 performers comparison

### Styling

- Dark theme optimized for financial data visualization
- Responsive design with modern UI components
- Professional color scheme with cyan accents

## 🔧 Configuration

### Adding New Tickers

To analyze additional stocks, update the `tickers` array in `config/config.json`:

```json
{
  "tickers": ["AAPL", "GOOG", "MSFT", "TSLA", "AMZN"]
}
```

### Data Storage Paths

Modify storage locations in `config/config.json`:

```json
{
  "paths": {
    "bronze": "data/bronze/stock_prices/",
    "silver": "data/silver/stock_prices/",
    "gold": "data/gold/stock_prices/"
  }
}
```

## 🐳 Docker Support

Build and run the application using Docker:

```bash
# Build the image
docker build -t alpha-vantage-dashboard .

# Run the container
docker run -p 8050:8050 alpha-vantage-dashboard
```

## 📈 Performance Considerations

- **Delta Lake**: Provides ACID transactions and time travel capabilities
- **Partitioning**: Data is partitioned by ticker for efficient querying
- **Spark Optimization**: Uses window functions for efficient calculations
- **Caching**: Spark automatically caches frequently accessed data
- **Rate Limiting**: Respects Alpha Vantage API limits to avoid throttling

## 🧪 Development

### Running Tests

```bash
pytest etl/
```

### Jupyter Notebook

For interactive analysis and prototyping:

```bash
jupyter lab notebooks/prototyping.ipynb
```

### Code Quality

The project follows Python best practices:

- Type hints where appropriate
- Comprehensive error handling
- Modular architecture with clear separation of concerns
- Detailed logging and status messages

## 📝 API Rate Limits

- **Alpha Vantage Free Tier**: 5 API calls per minute, 500 calls per day
- **Rate Limiting**: 12-second delays between API calls implemented
- **Error Handling**: Graceful handling of API failures and rate limit exceeded

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [Alpha Vantage](https://www.alphavantage.co/) for providing financial data API
- [Apache Spark](https://spark.apache.org/) for distributed data processing
- [Delta Lake](https://delta.io/) for data lakehouse capabilities
- [Dash](https://dash.plotly.com/) for interactive web applications

## 📞 Support

For questions, issues, or contributions, please:

- Open an issue on GitHub
- Check the documentation in the `notebooks/` directory
- Review the configuration examples in `config/`

---

**Note**: This project is for educational and research purposes. Please ensure compliance with Alpha Vantage's terms of service and any applicable financial regulations.
