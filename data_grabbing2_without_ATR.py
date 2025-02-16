from ib_insync import IB, Stock
import sqlite3
from datetime import datetime, timedelta
import pandas as pd

# Define investment dates and symbols
investment_data = [
    ("QMCO", "2025-02-12"),
    ("QMCO", "2025-02-12"),
    ("BSLK", "2025-02-13")
]

# Connect to IB Gateway
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=2)

def fetch_relative_volume(symbol, date):
    contract = Stock(symbol, 'SMART', 'USD')
    target_date = datetime.strptime(date, '%Y-%m-%d')

    # Get historical volume for the target date
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=target_date.strftime('%Y%m%d 23:59:59'),
        durationStr='1 D',
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    # Get historical volume for the past 10 days (excluding the target date)
    bars_10d = ib.reqHistoricalData(
        contract,
        endDateTime=(target_date - timedelta(days=1)).strftime('%Y%m%d 23:59:59'),
        durationStr='10 D',
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    if not bars or len(bars_10d) < 10:
        return None, None, None

    # Extract volume data
    target_volume = bars[0].volume if bars else None
    avg_volume_10d = sum(bar.volume for bar in bars_10d) / len(bars_10d) if bars_10d else None

    # Calculate relative volume
    relative_volume = round(target_volume / avg_volume_10d, 2) if avg_volume_10d else None

    return relative_volume, target_volume, avg_volume_10d

def fetch_open_price(symbol, date):
    contract = Stock(symbol, 'SMART', 'USD')
    target_date = datetime.strptime(date, '%Y-%m-%d')

    # Get historical data for the target date
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=target_date.strftime('%Y%m%d 23:59:59'),
        durationStr='1 D',
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    if not bars:
        return None

    # Extract open price
    open_price = bars[0].open if bars else None
    return open_price

def fetch_gap_and_changes(symbol, date):
    contract = Stock(symbol, 'SMART', 'USD')
    target_date = datetime.strptime(date, '%Y-%m-%d')

    # Get historical bars
    bars_daily = ib.reqHistoricalData(
        contract,
        endDateTime=(target_date + timedelta(days=1)).strftime('%Y%m%d 23:59:59'),
        durationStr='15 D',
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    if not bars_daily or len(bars_daily) < 8:
        return None, None, None, None, None

    # Extract necessary prices for gap and change calculations
    yesterday_close = bars_daily[-3].close
    today_open = bars_daily[-2].open
    today_close = bars_daily[-2].close
    tomorrow_open = bars_daily[-1].open

    # Calculate gap percentages
    gap_today = round(((today_open - yesterday_close) / yesterday_close) * 100, 2)
    gap_tomorrow = round(((tomorrow_open - today_close) / today_close) * 100, 2)

    # Calculate change percentages
    change_from_open = round(((today_close - today_open) / today_open) * 100, 2)

    # Calculate change for week
    if len(bars_daily) >= 8:
        week_ago_close = bars_daily[-8].close  # Close price from 7 trading days ago
        change_for_week = round(((today_close - week_ago_close) / week_ago_close) * 100, 2)
    else:
        change_for_week = None

    return gap_today, gap_tomorrow, change_from_open, change_for_week, today_close

def fetch_data(symbol, date):
    relative_volume, target_volume, avg_volume_10d = fetch_relative_volume(symbol, date)
    open_price = fetch_open_price(symbol, date)
    gap_today, gap_tomorrow, change_from_open, change_for_week, close_price = fetch_gap_and_changes(symbol, date)

    return {
        "Symbol": symbol,
        "Date": date,
        "Open Price": open_price,
        "Close Price": close_price,
        "Relative Volume": relative_volume,
        "Volume": target_volume,
        "10-Day Avg Volume": avg_volume_10d,
        "Gap Today %": f"{gap_today:.2f}%" if gap_today is not None else "N/A",
        "Gap Tomorrow %": f"{gap_tomorrow:.2f}%" if gap_tomorrow is not None else "N/A",
        "Change From Open %": f"{change_from_open:.2f}%" if change_from_open is not None else "N/A",
        "Change For Week %": f"{change_for_week:.2f}%" if change_for_week is not None else "N/A"
    }

# Fetch data for all investments
historical_data = [fetch_data(symbol, date) for symbol, date in investment_data]
historical_data = [data for data in historical_data if data is not None]

# Save to SQL database
conn = sqlite3.connect("historical_data.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_data (
        Symbol TEXT,
        Date TEXT,
        OpenPrice REAL,
        ClosePrice REAL,
        RelativeVolume REAL,
        Volume INTEGER,
        AvgVolume10D REAL,
        GapToday TEXT,
        GapTomorrow TEXT,
        ChangeFromOpen TEXT,
        ChangeForWeek TEXT
    )
""")

cursor.executemany("""
    INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?,?,?)
""", [tuple(data.values()) for data in historical_data])

conn.commit()
conn.close()

# Print data with labels
for data in historical_data:
    print("Stock Data:")
    for key, value in data.items():
        print(f"{key}: {value}")
    print("-" * 20)
