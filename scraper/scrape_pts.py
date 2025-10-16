# scraper/scrape_pts.py

# -*- coding: utf-8 -*-
"""
Kabutan PTS 夜間銘柄スクレイパー（GitHub Actions用）
出典: https://kabutan.jp/warning/pts_night_volume_ranking
出力: public/latest.json
"""

import os
import time
import datetime as dt
import requests
import pandas as pd
from bs4 import BeautifulSoup
import jpholiday
import json
from concurrent.futures import ThreadPoolExecutor, as_completed



# -----------------------------
# 実行条件：17〜翌6時台かつ、「今日 or 昨日が平日（営業日）」
# -----------------------------
now = dt.datetime.now()
today = now.date()
yesterday = today - dt.timedelta(days=1)

# 実行時間帯チェック
if not (17 <= now.hour or now.hour < 7):
    print(f"実行停止: 現在{now.strftime('%H:%M')}はPTS時間外。既存JSONを維持して終了します。")
    # latest.json が存在しない場合だけダミー出力しておく（初回保険）
    if not os.path.exists("public/latest.json"):
        os.makedirs("public", exist_ok=True)
        with open("public/latest.json", "w", encoding="utf-8") as f:
            json.dump({"message": "初回: データ未取得（PTS時間外）"}, f, ensure_ascii=False, indent=2)
    exit(0)

# 平日チェック（今日 or 昨日が平日ならOK）
today_is_weekday = (today.weekday() < 5 and not jpholiday.is_holiday(today))
yesterday_is_weekday = (yesterday.weekday() < 5 and not jpholiday.is_holiday(yesterday))

if not (today_is_weekday or yesterday_is_weekday):
    print(f"実行停止: {today} と {yesterday} はいずれも休場日。既存JSONを維持して終了します。")
    if not os.path.exists("public/latest.json"):
        os.makedirs("public", exist_ok=True)
        with open("public/latest.json", "w", encoding="utf-8") as f:
            json.dump({"message": "初回: データ未取得（休場日）"}, f, ensure_ascii=False, indent=2)
    exit(0)

print(f"✅ 実行開始: {now.strftime('%Y-%m-%d %H:%M')}（PTS時間内）")

# -----------------------------
# 設定
# -----------------------------
BASE_URL = "https://kabutan.jp/warning/pts_night_volume_ranking?market=0&capitalization=-1&dispmode=normal&stc=&stm=0&page="
DATE_STR = yesterday.strftime("%Y-%m-%d")

os.makedirs("public", exist_ok=True)

# -----------------------------
# スクレイピング関数
# -----------------------------
def scrape_page(page):
    url = BASE_URL + str(page)
    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser")

    table = soup.select_one("#main > div:nth-of-type(5) > table")
    if table is None:
        return None

    rows = []
    for tr in table.select("tbody tr"):
        code = tr.select_one("td.tac a")
        name = tr.select_one("th.tal")
        market = tr.select("td.tac")
        if not code or not name:
            continue

        market_text = market[1].get_text(strip=True) if len(market) > 1 else ""
        data_cells = [td.get_text(strip=True) for td in tr.find_all("td")[4:]]

        row = [code.get_text(strip=True), name.get_text(strip=True), market_text] + data_cells
        rows.append(row)
    return rows

# -----------------------------
# 全ページ並列取得
# -----------------------------
def scrape_all_pages(max_pages=10):
    all_rows = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 最大5並列（Kabutanに過負荷をかけすぎない範囲）
        futures = {executor.submit(scrape_page, p): p for p in range(1, max_pages + 1)}
        for future in as_completed(futures):
            page = futures[future]
            try:
                rows = future.result()
                if not rows:
                    continue
                print(f"{page}ページ取得完了: {len(rows)}件")
                all_rows.extend(rows)
            except Exception as e:
                print(f"{page}ページでエラー: {e}")
    return all_rows

print("PTSデータ取得開始...")
all_rows = scrape_all_pages(max_pages=10)  # ← ページ数は適宜調整
print(f"総件数: {len(all_rows)}")


# -----------------------------
# DataFrame化
# -----------------------------
columns = [
    "コード", "銘柄名", "市場",
    "通常取引終値", "PTS株価", "通常比", "変化率",
    "出来高", "PER", "PBR", "利回り"
]
df = pd.DataFrame(all_rows, columns=columns)
df.insert(0, "日付", DATE_STR)

from datetime import datetime, timezone, timedelta
JST = timezone(timedelta(hours=9))
now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S JST")


# -----------------------------
# JSON出力
# -----------------------------
os.makedirs("public", exist_ok=True)
json_path = os.path.join("public", "latest.json")

bundle = {
    "generated_at": now,
    "records": df.to_dict(orient="records")
}

with open(json_path, "w", encoding="utf-8") as f:
    json.dump(bundle, f, ensure_ascii=False, indent=2)

print(f"JSON出力完了: {json_path}（生成時刻: {now}）")







