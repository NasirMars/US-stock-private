from ib_insync import IB, Stock
import sqlite3
from datetime import datetime, timedelta
import pandas as pd

# Define investment dates and symbols
investment_data = [
    ("TPIC", "2024-12-20"),
    ("NVNI", "2024-12-19"),
    ("NVNI", "2024-12-20"),
    ("NVNI", "2024-12-23"),
    ("NVNI", "2024-12-16"),
    ("FRSX", "2024-12-20"),
    ("BIOA", "2024-12-20"),
    ("OPTX", "2024-12-20"),
    ("RCAT", "2024-12-20")
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

def fetch_gap_and_changes(symbol, target_date):  # target_date is ALREADY a datetime object
    contract = Stock(symbol, 'SMART', 'USD')

    yesterday = target_date - timedelta(days=1)
    while yesterday.weekday() > 4:  # Skip weekends
        yesterday -= timedelta(days=1)

    next_trading_day = target_date + timedelta(days=1)
    while next_trading_day.weekday() > 4:  # Skip weekends
        next_trading_day += timedelta(days=1)

    bars_daily = ib.reqHistoricalData(
        contract,
        endDateTime=(next_trading_day + timedelta(days=1)).strftime('%Y%m%d 23:59:59'),
        durationStr='15 D',
        barSizeSetting='1 day',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )

    if not bars_daily or len(bars_daily) < 8:
        return None, None, None, None, None

    yesterday_bar = next((bar for bar in bars_daily if bar.date == yesterday.date()), None)
    today_bar = next((bar for bar in bars_daily if bar.date == target_date.date()), None)
    next_day_bar = next((bar for bar in bars_daily if bar.date == next_trading_day.date()), None)

    if not yesterday_bar or not today_bar:
        return None, None, None, None, None

    yesterday_close = yesterday_bar.close
    today_open = today_bar.open
    today_close = today_bar.close

    if next_day_bar:
        tomorrow_open = next_day_bar.open
        gap_tomorrow = round(((tomorrow_open - today_close) / today_close) * 100, 2)
    else:
        gap_tomorrow = None

    gap_today = round(((today_open - yesterday_close) / yesterday_close) * 100, 2)
    change_from_open = round(((today_close - today_open) / today_open) * 100, 2)

    if len(bars_daily) >= 8:
        week_ago_close = bars_daily[-8].close
        change_for_week = round(((today_close - week_ago_close) / week_ago_close) * 100, 2)
    else:
        change_for_week = None

    return gap_today, gap_tomorrow, change_from_open, change_for_week, today_close


def fetch_data(symbol, date_str):
    target_date = datetime.strptime(date_str, '%Y-%m-%d')  # Convert date_str to datetime object
    relative_volume, target_volume, avg_volume_10d = fetch_relative_volume(symbol, date_str)
    open_price = fetch_open_price(symbol, date_str)

    # Pass the correct datetime object
    gap_today, gap_tomorrow, change_from_open, change_for_week, close_price = fetch_gap_and_changes(symbol, target_date)

    return {
        "Symbol": symbol,
        "Date": date_str,
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
