import yfinance as yf
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# =========================
# 1️⃣ 종목 리스트 (중복 제거)
# =========================
tickers = [
    "MRNA", "IQV", "VRTX", "BEAM", "CRSP", "NTLA", "TEM", "GEN", "ISRG",
    "AMBA", "ASML", "TSM", "AMD", "INTC",
    "ROKU", "RBLX", "CRWD", "PANW", "FTNT", "CSCO", "SNOW", "SNPS",
    "CORZ", "SHOP", "SPOT", "SPIR", "PL", "RKLB", "BWXT", "RDW",
    "KTOS", "AVAV", "BULL", "BITF", "COIN", "HOOD", "BREA",
    "VRT", "ETN",
    "263750.KQ", "032830.KS", "005930.KS", "000660.KS", "012450.KS",
    "064350.KS", "034020.KS",
    "IBM", "TSLA", "AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META",
    "AVGO", "PLTR"
]

# 중복 제거 (순서 유지)
tickers = list(dict.fromkeys(tickers))

# =========================
# 2️⃣ 저장 폴더 생성
# =========================
SAVE_FOLDER = "daily"
os.makedirs(SAVE_FOLDER, exist_ok=True)

print(f"⭐ 총 {len(tickers)}개 종목 수집 시작\n")

# =========================
# 3️⃣ 기준 컬럼 정의
# =========================
COLUMNS = ["Date", "Open", "Close", "Volume"]

# =========================
# 4️⃣ 데이터 수집 루프
# =========================
for ticker in tickers:
    try:
        # ⏳ Yahoo 차단 방지 (Actions 필수)
        time.sleep(1.5)

        # 🔥 yfinance 안정 호출
        data = yf.download(
            ticker,
            period="7d",        # 최근 7일
            interval="1d",
            progress=False,
            threads=False
        )

        # 데이터 정리
        data = data.dropna()

        if data.empty:
            print(f"⚠️ 데이터 없음 → 스킵 ({ticker})")
            continue

        # ✅ 가장 최신 거래일 기준
        latest = data.iloc[-1]
        date = data.index[-1].strftime("%Y-%m-%d")

        open_price = float(latest["Open"])
        close_price = float(latest["Close"])
        volume = int(latest["Volume"])

        save_path = os.path.join(SAVE_FOLDER, f"{ticker}.csv")

        # CSV 로드 or 새로 생성
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)

                # 컬럼 구조 깨졌을 경우 복구
                if list(df.columns) != COLUMNS:
                    df = pd.DataFrame(columns=COLUMNS)

            except Exception:
                df = pd.DataFrame(columns=COLUMNS)
        else:
            df = pd.DataFrame(columns=COLUMNS)

        # 🚫 같은 날짜 중복 저장 방지
        if date in df["Date"].values:
            print(f"⏭️ 이미 저장됨 ({ticker}) {date}")
            continue

        # 데이터 추가
        df.loc[len(df)] = [date, open_price, close_price, volume]
        df.to_csv(save_path, index=False)

        print(f"✅ 저장 완료: {ticker} ({date})")

    except Exception as e:
        print(f"❌ 오류 발생 ({ticker}): {e}")

print("\n🎉 전체 종목 수집 완료!")
