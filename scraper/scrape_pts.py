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

# -----------------------------
# 実行条件：昨日が平日（営業日）の翌朝のみ
# -----------------------------
today = dt.date.today()
yesterday = today - dt.timedelta(days=1)

if yesterday.weekday() >= 5 or jpholiday.is_holiday(yesterday):
    print(f"停止: {yesterday} は休場日です。")
    exit()

print(f"{yesterday} は営業日。PTSスクレイピングを実行します。")

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
# 全ページ取得
# -----------------------------
all_rows = []
page = 1
while True:
    rows = scrape_page(page)
    if not rows:
        break
    all_rows.extend(rows)
    print(f"{page}ページ取得完了: {len(rows)}件")
    page += 1
    time.sleep(1)

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

# -----------------------------
# JSON出力
# -----------------------------
json_path = os.path.join("public", "latest.json")
df.to_json(json_path, orient="records", force_ascii=False, indent=2)
print(f"JSON出力完了: {json_path}")
