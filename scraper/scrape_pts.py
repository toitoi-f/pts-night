# -*- coding: utf-8 -*-
"""
Kabutan PTS（夜間）スクレイパー → JSON出力
- 対象: 値上がり/値下がり/出来高 (夜間PTSの警戒ページ)
- 出力: public/pts.json
- 実行環境: GitHub Actions (5分間隔)
注意: スクレイピングはサイト規約を尊重してください。頻度は5分と控えめ・ヘッダ付与。
"""

import os, json, time
import datetime as dt
from typing import List, Dict
import requests
from bs4 import BeautifulSoup

JST = dt.timezone(dt.timedelta(hours=9), name="JST")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; pts-bot/1.0; +https://github.com/yourname/pts-night)"
}

TARGETS = [
    # 株探の構成は変わり得るため、必要に応じて増減・修正してください
    ("night_up",   "https://kabutan.jp/warning/pts_night_price_increase"),
    ("night_down", "https://kabutan.jp/warning/pts_night_price_decrease"),
    ("night_vol",  "https://kabutan.jp/warning/pts_night_volume"),
]

def fetch_html(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def parse_table(soup: BeautifulSoup) -> List[Dict]:
    """
    株探の警戒ページはだいたい「table > tbody > tr > td」構造。
    列の並びは変動し得るので、主要情報は安全に抽出。
    """
    data = []
    table = soup.find("table")
    if not table:
        return data
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not tds or len(tds) < 3:
            continue

        # ざっくり安全に抽出（列変動に強め）
        # 想定: [順位, コード, 銘柄名, 市場, 現在値, 前日比, 前日比%, 出来高, 時刻, ...]
        def safe(idx, default=""):
            try:
                return tds[idx]
            except IndexError:
                return default

        code = safe(1)
        name = safe(2)
        market = safe(3)
        price = safe(4)
        diff  = safe(5)
        rate  = safe(6)
        vol   = safe(7)
        at    = safe(8)

        # 数値整形はフロント側でもできるのでここでは文字列のまま
        data.append({
            "code": code,
            "name": name,
            "market": market,
            "price": price,
            "diff": diff,
            "diff_rate": rate,
            "volume": vol,
            "time": at,
        })
    return data

def main():
    bundle = {
        "generated_at": dt.datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "sources": [],
        "items": {},   # カテゴリ別
    }
    for key, url in TARGETS:
        try:
            soup = fetch_html(url)
            items = parse_table(soup)
            bundle["items"][key] = items
            bundle["sources"].append({"key": key, "url": url})
            time.sleep(1.0)  # 礼儀として軽くウェイト
        except Exception as e:
            bundle["items"][key] = []
            bundle["sources"].append({"key": key, "url": url, "error": str(e)})

    os.makedirs("public", exist_ok=True)
    with open("public/pts.json", "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
