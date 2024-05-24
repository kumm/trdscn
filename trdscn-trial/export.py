from pandas import DataFrame
import chart_db

# candles = chart_db.load_daily_candles("BNNCUMF:BTCUSDT")
candles = chart_db.load_daily_candles("NYSE:SPY")

df = DataFrame(data=candles)
df.drop(columns=df.columns[0], axis=1, inplace=True)
df['ticker'] = "SPY"
print(df)
df.to_csv("spy.csv")
