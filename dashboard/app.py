import dash
from dash import dcc, html, Input, Output
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from etl.spark_utils import get_spark
from etl.config_loader import load_config

try:
    config = load_config()
    spark = get_spark("PlotlyDashboard")

    gold_path = config["paths"]["gold"]
except Exception as e:
    print(f"❌ Error starting spark session: {e}")

try:
    df = spark.read.format("delta").load(gold_path)
    tickers = [row["ticker"] for row in df.select("ticker").distinct().collect()]
except Exception as e:
    print(f"❌ Error fetching gold layer data: {e}")

def get_ticker_data(ticker):
    return (
        df.filter(df.ticker == ticker)
            .orderBy("date")
            .toPandas()
        )

# Initialize Dash app
app = dash.Dash(__name__)
app.title = "Stock Analysis Dashboard"

# Professional styling inspired by Robinhood
app.layout = html.Div(
    style={
        "backgroundColor": "#0a0f1f",
        "color": "#EAEAEA",
        "fontFamily": "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        "minHeight": "100vh",
        "padding": "0"
    },
    children=[
        # Header Section
        html.Div([
            html.Div([
                html.H1("Stock Analysis Dashboard",
                        style={
                            "color": "#00E0FF", 
                            "fontSize": "32px",
                            "fontWeight": "700",
                            "margin": "0",
                            "letterSpacing": "-0.5px"
                        }),
                html.P("Professional market analysis and insights",
                       style={
                           "color": "#8B949E",
                           "fontSize": "16px",
                           "margin": "8px 0 0 0",
                           "fontWeight": "400"
                       })
            ], style={"textAlign": "center", "padding": "32px 0 24px 0"}),
            
            # Controls Section - Horizontal Layout
            html.Div([
                html.Div([
                    html.Label("Select Stock", 
                              style={
                                  "color": "#EAEAEA",
                                  "fontSize": "14px",
                                  "fontWeight": "600",
                                  "marginBottom": "8px",
                                  "display": "block"
                              }),
                    dcc.Dropdown(
                        id="ticker-dropdown",
                        options=[{"label": t, "value": t} for t in tickers],
                        value=tickers[0],
                        clearable=False,
                        style={
                            "backgroundColor": "#1a1a1a",
                            "border": "1px solid #00E0FF",
                            "borderRadius": "8px",
                            "color": "#00E0FF"
                        }
                    )
                ], style={"width": "200px", "marginRight": "24px"}),
                
                html.Div([
                    html.Label("Date Range", 
                              style={
                                  "color": "#EAEAEA",
                                  "fontSize": "14px",
                                  "fontWeight": "600",
                                  "marginBottom": "8px",
                                  "display": "block"
                              }),
                    dcc.DatePickerRange(
                        id="date-range",
                        start_date=df.agg({"date": "min"}).collect()[0][0],
                        end_date=df.agg({"date": "max"}).collect()[0][0],
                        display_format="MMM DD, YYYY",
                        style={
                            "backgroundColor": "#1a1a1a",
                            "border": "1px solid #00E0FF",
                            "borderRadius": "8px",
                            "color": "#00E0FF",
                            "fontSize": "14px",
                            "padding": "8px 12px",
                            "width": "100%"
                        }
                    )
                ], style={"width": "300px"})
            ], style={
                "display": "flex", 
                "justifyContent": "center", 
                "alignItems": "flex-end",
                "marginBottom": "32px",
                "padding": "0 24px"
            })
        ], style={
            "backgroundColor": "#0d1117",
            "borderBottom": "1px solid #21262d",
            "marginBottom": "24px"
        }),

        # Main Content Container
        html.Div([
            # KPI Cards Section
            html.Div(id="kpi-cards",
                     style={
                         "display": "grid",
                         "gridTemplateColumns": "repeat(auto-fit, minmax(280px, 1fr))",
                         "gap": "16px",
                         "marginBottom": "32px",
                         "padding": "0 24px"
                     }),

            # Charts Section
            html.Div([
                # Price Chart Card
                html.Div([
                    dcc.Graph(id="price-chart",
                              config={"displayModeBar": False},
                              style={"height": "500px"})
                ], style={
                    "backgroundColor": "#0d1117",
                    "border": "1px solid #21262d",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "marginBottom": "24px",
                    "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1)"
                }),

                # Secondary Charts Row
                html.Div([
                    html.Div([
                        dcc.Graph(id="return-chart",
                                  config={"displayModeBar": False},
                                  style={"height": "350px"})
                    ], style={
                        "backgroundColor": "#0d1117",
                        "border": "1px solid #21262d",
                        "borderRadius": "12px",
                        "padding": "20px",
                        "marginRight": "12px",
                        "flex": "1",
                        "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1)"
                    }),
                    
                    html.Div([
                        dcc.Graph(id="volatility-chart",
                                  config={"displayModeBar": False},
                                  style={"height": "350px"})
                    ], style={
                        "backgroundColor": "#0d1117",
                        "border": "1px solid #21262d",
                        "borderRadius": "12px",
                        "padding": "20px",
                        "marginLeft": "12px",
                        "flex": "1",
                        "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1)"
                    })
                ], style={
                    "display": "flex",
                    "marginBottom": "24px",
                    "padding": "0 24px"
                }),

                # Leaderboard Chart Card
                html.Div([
                    dcc.Graph(id="leaderboard-chart",
                              config={"displayModeBar": False},
                              style={"height": "400px"})
                ], style={
                    "backgroundColor": "#0d1117",
                    "border": "1px solid #21262d",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1)"
                })
            ], style={"padding": "0 24px"})
        ])
    ]
)


def style_fig(fig, title):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EAEAEA", family="'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"),
        title=dict(
            text=title, 
            x=0.5, 
            font=dict(color="#00E0FF", size=18, family="'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"),
            xanchor="center",
            y=0.95
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor="#21262d",
            gridwidth=1,
            color="#8B949E",
            title_font=dict(size=12, color="#EAEAEA"),
            tickfont=dict(size=11, color="#8B949E")
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#21262d", 
            gridwidth=1,
            color="#8B949E",
            title_font=dict(size=12, color="#EAEAEA"),
            tickfont=dict(size=11, color="#8B949E")
        ),
        margin=dict(l=20, r=20, t=60, b=20),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=12, color="#EAEAEA"),
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0,0,0,0)"
        )
    )
    return fig

def make_returns_fig(pdf, ticker):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # daily return on primary y
    fig.add_trace(
        go.Scatter(
            x=pdf["date"], 
            y=pdf["ret"], 
            mode="lines", 
            name="Daily Return",
            line=dict(color="#00E0FF", width=2),
            hovertemplate="<b>Daily Return</b><br>Date: %{x}<br>Return: %{y:.2%}<extra></extra>"
        ),
        secondary_y=False
    )
    # cumulative return (%) on secondary y
    fig.add_trace(
        go.Scatter(
            x=pdf["date"], 
            y=pdf["cum_ret"], 
            mode="lines", 
            name="Cumulative Return",
            line=dict(color="#00D4AA", width=3),
            hovertemplate="<b>Cumulative Return</b><br>Date: %{x}<br>Return: %{y:.2%}<extra></extra>"
        ),
        secondary_y=True
    )
    fig.update_yaxes(
        title_text="Daily Return", 
        tickformat=".1%", 
        secondary_y=False,
        title_font=dict(size=12, color="#EAEAEA")
    )
    fig.update_yaxes(
        title_text="Cumulative Return", 
        tickformat=".0%", 
        secondary_y=True,
        title_font=dict(size=12, color="#EAEAEA")
    )
    return style_fig(fig, f"{ticker} Returns")

def make_vol_fig(pdf, ticker):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pdf["date"], 
        y=pdf["vol20"], 
        mode="lines", 
        name="20d Volatility",
        line=dict(color="#FF6B6B", width=2),
        hovertemplate="<b>20d Volatility</b><br>Date: %{x}<br>Volatility: %{y:.2%}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=pdf["date"], 
        y=pdf["vol50"], 
        mode="lines", 
        name="50d Volatility",
        line=dict(color="#FFA500", width=2),
        hovertemplate="<b>50d Volatility</b><br>Date: %{x}<br>Volatility: %{y:.2%}<extra></extra>"
    ))
    # vol is annualized (e.g., 0.25 = 25%)
    fig.update_yaxes(
        title="Annualized Volatility", 
        tickformat=".0%",
        title_font=dict(size=12, color="#EAEAEA")
    )
    return style_fig(fig, f"{ticker} Volatility")

@app.callback(
    [
        Output("price-chart", "figure"),
        Output("return-chart", "figure"),
        Output("volatility-chart", "figure"),
        Output("leaderboard-chart", "figure"),
        Output("kpi-cards", "children")
    ],
    [
        Input("ticker-dropdown", "value"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date")
    ]
)

def update_charts(ticker, start_date, end_date):
    pdf = get_ticker_data(ticker).sort_values("date")

    # 🔧 Fix date filtering
    if start_date and end_date:
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        pdf["date"] = pd.to_datetime(pdf["date"])
        pdf = pdf[(pdf["date"] >= start_date) & (pdf["date"] <= end_date)]


    latest_row = pdf.iloc[-1]

    # Determine color for cumulative return
    return_color = "#00D4AA" if latest_row['cum_ret'] >= 0 else "#FF6B6B"
    
    kpis = [
        html.Div([
            html.Div([
                html.H3("Last Close", style={
                    "color": "#8B949E",
                    "fontSize": "14px",
                    "fontWeight": "600",
                    "margin": "0 0 8px 0",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.5px"
                }),
                html.P(f"${latest_row['close']:.2f}", style={
                    "color": "#EAEAEA",
                    "fontSize": "28px",
                    "fontWeight": "700",
                    "margin": "0",
                    "fontFamily": "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
                })
            ], style={"textAlign": "center"})
        ], style={
            "backgroundColor": "#0d1117",
            "border": "1px solid #21262d",
            "borderRadius": "12px",
            "padding": "24px",
            "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1)",
            "transition": "all 0.2s ease-in-out"
        }),

        html.Div([
            html.Div([
                html.H3("Cumulative Return", style={
                    "color": "#8B949E",
                    "fontSize": "14px",
                    "fontWeight": "600",
                    "margin": "0 0 8px 0",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.5px"
                }),
                html.P(f"{latest_row['cum_ret']:.2%}", style={
                    "color": return_color,
                    "fontSize": "28px",
                    "fontWeight": "700",
                    "margin": "0",
                    "fontFamily": "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
                })
            ], style={"textAlign": "center"})
        ], style={
            "backgroundColor": "#0d1117",
            "border": "1px solid #21262d",
            "borderRadius": "12px",
            "padding": "24px",
            "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1)",
            "transition": "all 0.2s ease-in-out"
        }),

        html.Div([
            html.Div([
                html.H3("20d Volatility", style={
                    "color": "#8B949E",
                    "fontSize": "14px",
                    "fontWeight": "600",
                    "margin": "0 0 8px 0",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.5px"
                }),
                html.P(f"{latest_row['vol20']:.2%}", style={
                    "color": "#EAEAEA",
                    "fontSize": "28px",
                    "fontWeight": "700",
                    "margin": "0",
                    "fontFamily": "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
                })
            ], style={"textAlign": "center"})
        ], style={
            "backgroundColor": "#0d1117",
            "border": "1px solid #21262d",
            "borderRadius": "12px",
            "padding": "24px",
            "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1)",
            "transition": "all 0.2s ease-in-out"
        })
    ]

    # --- Price chart with SMAs ---
    price_fig = go.Figure()
    price_fig.add_trace(go.Candlestick(
        x=pdf["date"], 
        open=pdf["open"], 
        high=pdf["high"],
        low=pdf["low"], 
        close=pdf["close"], 
        name="Price",
        increasing_line_color="#00D4AA",
        decreasing_line_color="#FF6B6B",
        increasing_fillcolor="#00D4AA",
        decreasing_fillcolor="#FF6B6B"
    ))
    price_fig.add_trace(go.Scatter(
        x=pdf["date"], 
        y=pdf["sma20"], 
        name="20d SMA",
        line=dict(color="#00E0FF", width=2),
        hovertemplate="<b>20d SMA</b><br>Date: %{x}<br>Price: $%{y:.2f}<extra></extra>"
    ))
    price_fig.add_trace(go.Scatter(
        x=pdf["date"], 
        y=pdf["sma50"], 
        name="50d SMA",
        line=dict(color="#FFA500", width=2),
        hovertemplate="<b>50d SMA</b><br>Date: %{x}<br>Price: $%{y:.2f}<extra></extra>"
    ))
    price_fig.update_layout(
        xaxis_rangeslider_visible=False,
        yaxis_title="Price ($)",
        xaxis_title="Date"
    )
    price_fig = style_fig(price_fig, f"{ticker} Price + Moving Averages")

    return_fig = make_returns_fig(pdf, ticker)
    vol_fig    = make_vol_fig(pdf, ticker)

    # --- Leaderboard chart (multi-ticker comparison) ---
    # ⚡ For this one we need all tickers, not just selected
    latest = df.groupBy("ticker").agg(F.max("date").alias("date"))
    latest_df = df.join(latest, ["ticker", "date"])
    all_pdf = latest_df.toPandas()

    latest_date = all_pdf["date"].max()
    perf = (
        all_pdf[all_pdf["date"] == latest_date]
        .groupby("ticker")["cum_ret"].last()
        .sort_values(ascending=False)
    )

    top5 = perf.head(5)
    bottom5 = perf.tail(5)

    leaderboard_fig = go.Figure()
    leaderboard_fig.add_trace(go.Bar(
        x=top5.values, 
        y=top5.index, 
        orientation="h", 
        name="Top Performers",
        marker_color="#00D4AA",
        hovertemplate="<b>%{y}</b><br>Return: %{x:.2%}<extra></extra>"
    ))
    leaderboard_fig.add_trace(go.Bar(
        x=bottom5.values, 
        y=bottom5.index, 
        orientation="h", 
        name="Bottom Performers",
        marker_color="#FF6B6B",
        hovertemplate="<b>%{y}</b><br>Return: %{x:.2%}<extra></extra>"
    ))
    leaderboard_fig.update_layout(
        xaxis_title="Cumulative Return",
        yaxis_title="Ticker",
        xaxis_title_font=dict(size=12, color="#EAEAEA"),
        yaxis_title_font=dict(size=12, color="#EAEAEA")
    )
    leaderboard_fig = style_fig(leaderboard_fig, "Performance Leaderboard")

    return price_fig, return_fig, vol_fig, leaderboard_fig, kpis

if __name__ == "__main__":
    app.run_server(debug=True, port=8050)
