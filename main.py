import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime

tickers = [
    "MRNA", "IQV", "VRTX", "BEAM", "CRSP", "NTLA", "TEM", "GEN", "ISRG",
    "AMBA", "ASML", "TSM", "AMD", "INTC",
    "ROKU", "RBLX", "CRWD", "PANW", "FTNT", "CSCO", "SNOW", "SNPS", "CORZ", "SHOP", "SPOT",
    "SPIR", "PL", "RKLB", "BWXT", "RDW", "KTOS", "AVAV",
    "BULL", "BITF", "COIN", "HOOD", "BREA",
    "VRT", "ETN",
    "263750.KQ", "032830.KS", "005930.KS", "000660.KS", "012450.KS",
    "064350.KS", "034020.KS", "IBM", "TSLA", "AAPL", "NVDA",
    "MSFT", "GOOGL", "AMZN", "META", "AVGO", "PLTR", "INTC"
]

save_folder = "daily"
os.makedirs(save_folder, exist_ok=True)

print(f"⭐ {len(tickers)}개 종목 데이터 기록 시작...\n")

for ticker in tickers:
    try:
        time.sleep(0.5)

        data = yf.download(ticker, period="1d", interval="1d")

        if data.empty:
            print(f"⚠️ 데이터 없음 ({ticker}) — 스킵됨")
            continue

        close = float(data["Close"].iloc[-1])
        openp = float(data["Open"].iloc[-1])
        volume = int(data["Volume"].iloc[-1])
        date = datetime.now().strftime("%Y-%m-%d")

        save_path = os.path.join(save_folder, f"{ticker}.csv")

        # 항상 동일한 컬럼 구조를 강제
        columns = ["Date", "Open", "Close", "Volume"]

        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)

                # CSV 헤더가 깨졌을 경우 자동 복구
                if list(df.columns) != columns:
                    df = pd.DataFrame(columns=columns)

            except Exception:
                df = pd.DataFrame(columns=columns)

        else:
            df = pd.DataFrame(columns=columns)

        df.loc[len(df)] = [date, openp, close, volume]
        df.to_csv(save_path, index=False)

        print(f"✅ 저장 완료: {ticker}")

    except Exception as e:
        print(f"⚠️ 오류 발생 ({ticker}): {e}")

print("\n🎉 모든 종목 기록 완료!")

