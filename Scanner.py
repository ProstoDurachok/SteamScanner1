# -*- coding: utf-8 -*-
import os
import re
import json
import time
import random
import requests
import pytz
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, SSLError, ProxyError
from matplotlib.ticker import FuncFormatter
import html
import io
from io import BytesIO
import argparse
import shutil

# ===================== НАСТРОЙКИ =====================
BYMYKEL_URL = "https://bymykel.github.io/CSGO-API/api/en/all.json"
LOCAL_DB = "items.json"
APPID = 730

# Telegram
TOKEN = os.environ.get("TELEGRAM_TOKEN", "8427688497:AAGkBisiTfJM3RDc8DOG9Kx9l9EnekoFGQk")
CHAT_ID = os.environ.get("CHAT_ID", "-1003143360650")

# Steam sessionid
SESSIONID = os.environ.get("STEAM_SESSIONID", None)

# Прокси
USE_PROXY_BY_DEFAULT = True
PROXY_HTTP_URL = "http://mm4pkP:a6K4yx@95.181.155.167:8000"
PROXY_HTTP_ALT = "http://lte6:LVxqnyQiMH@65.109.79.15:13014"

# Поведение запросов (увеличено для снижения 429)
REQUEST_DELAY = 12.0  # Было 8.5
JITTER = 2.0  # Было 1.5
MAX_RETRIES = 2
MAX_RETRIES_429 = 3  # Уменьшено, чтобы не затягивать
BACKOFF_BASE = 2.0
RATE_LIMIT_PAUSE = 30  # Пауза после 3-х 429 подряд
RATE_LIMIT_COUNT = 0  # Global счётчик для логов

# Фильтры
VOLATILITY_THRESHOLD = 8.0
PRICE_CHANGE_THRESHOLD = 8.0
BREAKOUT_THRESHOLD = 2.0
MIN_PRICE = 2.0
MIN_VOLUME_24H = 1
HISTORY_DAYS = 7
USD_RATE = 83.4  # Fallback значение

# Папка для CSV/PNG
OUT_DIR = "out"
os.makedirs(OUT_DIR, exist_ok=True)

# Таймзона
TZ = pytz.timezone("Europe/Moscow")
EEST_TZ = pytz.timezone("Europe/Tallinn")
DEFAULT_SUMMARY_TIME = "00:00"

# SSL
ALLOW_INSECURE = False

# Лог
LOG_FILE = "posted_items.json"
SUMMARY_LOG = "summary_log.json"

# ===================== ФУНКЦИЯ ПОЛУЧЕНИЯ КУРСА USD/RUB =====================
def get_usd_to_rub_rate():
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        usd_rate = data['Valute']['USD']['Value']
        print(f"[INFO] USD/RUB rate: {usd_rate:.2f}")
        return usd_rate
    except Exception as e:
        print(f"[WARN] Failed to fetch USD rate: {e}. Using fallback: {USD_RATE}")
        return USD_RATE  # Fallback на константу

# ===================== Сессия requests =====================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/118.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.5"
})
session.trust_env = False
session.verify = True
if SESSIONID:
    session.cookies.set("sessionid", SESSIONID)

def enable_proxy(proxy_url: str):
    global RATE_LIMIT_COUNT
    RATE_LIMIT_COUNT = 0  # Reset counter on proxy switch
    session.proxies.update({"http": proxy_url, "https": proxy_url})
    print(f"[INFO] Proxy enabled: {proxy_url}")

def disable_proxy():
    global RATE_LIMIT_COUNT
    RATE_LIMIT_COUNT = 0
    session.proxies.clear()
    print("[INFO] Proxy disabled (direct connection)")

USE_PROXY = USE_PROXY_BY_DEFAULT
if USE_PROXY:
    enable_proxy(PROXY_HTTP_URL)
else:
    disable_proxy()

# ===================== УТИЛИТЫ =====================
def safe_json_loads(s: str) -> Optional[Any]:
    try:
        s2 = s.replace("'", '"')
        s2 = re.sub(r",\s*]", "]", s2)
        s2 = re.sub(r",\s*}", "}", s2)
        return json.loads(s2)
    except Exception:
        return None

def parse_date(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            if value > 1e12:
                return datetime.fromtimestamp(value / 1000.0)
            return datetime.fromtimestamp(value)
        except:
            return None
    if isinstance(value, str):
        s = value.strip()
        s = re.sub(r"\s*\+?\d+$", "", s).strip()
        formats = [
            "%b %d %Y %H:%M:%S",
            "%b %d %Y %H:%M",
            "%b %d %Y %H:",
            "%Y-%m-%d %H:%M:%S",
            "%d %b %Y %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(s, fmt)
            except:
                pass
        m = re.match(r"([A-Za-z]{3}\s+\d{1,2}\s+\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", s)
        if m:
            try:
                base = m.group(1)
                hh = int(m.group(2)); mm = int(m.group(3)); ss = int(m.group(4)) if m.group(4) else 0
                dt = datetime.strptime(base, "%b %d %Y").replace(hour=hh, minute=mm, second=ss)
                return dt
            except:
                pass
    return None

# ===================== ЛОГ ПОСЛЕДНЕЙ СВОДКИ =====================
def load_last_summary() -> Optional[datetime]:
    if os.path.exists(SUMMARY_LOG):
        try:
            with open(SUMMARY_LOG, "r", encoding="utf-8") as f:
                data = json.load(f)
                return datetime.fromisoformat(data['last_sent'].replace('Z', '+00:00'))
        except Exception:
            pass
    return None

def save_last_summary(dt: datetime):
    with open(SUMMARY_LOG, "w", encoding="utf-8") as f:
        json.dump({'last_sent': dt.isoformat()}, f, ensure_ascii=False, indent=2)

# ===================== РЕКВЕСТЫ С RETRY / FALLBACK =====================
def request_with_retries(url: str, params=None, headers=None, timeout=15, allow_429_backoff=True, force_direct=False):
    global RATE_LIMIT_COUNT, USE_PROXY
    if force_direct:
        session.proxies.clear()
        print("[INFO] Force direct for request")
    
    attempt = 0
    consecutive_429 = 0
    while attempt <= MAX_RETRIES:
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429 and allow_429_backoff:
                consecutive_429 += 1
                RATE_LIMIT_COUNT += 1
                if RATE_LIMIT_COUNT % 10 == 0:
                    print(f"[WARN] Total 429 hits: {RATE_LIMIT_COUNT}. Consider slowing down.")
                attempt_429 = 0
                while attempt_429 < MAX_RETRIES_429:
                    attempt_429 += 1
                    backoff = BACKOFF_BASE ** attempt_429 + random.random()
                    print(f"[WARN] 429 rate limit, waiting {backoff:.1f}s (retry {attempt_429}/{MAX_RETRIES_429})")
                    time.sleep(backoff)
                    r = session.get(url, params=params, headers=headers, timeout=timeout)
                    if r.status_code != 429:
                        consecutive_429 = 0  # Reset on success
                        break
                if consecutive_429 >= 3:
                    print(f"[WARN] 3+ consecutive 429, pausing {RATE_LIMIT_PAUSE}s")
                    time.sleep(RATE_LIMIT_PAUSE)
                    consecutive_429 = 0
                    # Rotate proxy on repeated 429
                    if USE_PROXY:
                        print("[INFO] Rotating to alt proxy due to 429")
                        enable_proxy(PROXY_HTTP_ALT)
                        USE_PROXY = True  # Keep using alt
                    else:
                        print("[INFO] Switching to direct due to 429")
                        disable_proxy()
            return r
        except (SSLError, ProxyError) as err:
            consecutive_429 = 0  # Not 429
            print(f"[WARN] Proxy/SSL error: {type(err).__name__}. Rotating proxy.")
            old_proxies = session.proxies.copy()
            try:
                if "mm4pkP" in PROXY_HTTP_URL and USE_PROXY:  # If primary, switch to alt
                    print("[INFO] Switching to alt proxy")
                    enable_proxy(PROXY_HTTP_ALT)
                else:
                    print("[INFO] Falling back to direct")
                    disable_proxy()
                r = session.get(url, params=params, headers=headers, timeout=timeout)
                if r.status_code == 429 and allow_429_backoff:
                    attempt_429 = 0
                    while attempt_429 < MAX_RETRIES_429:
                        attempt_429 += 1
                        backoff = BACKOFF_BASE ** attempt_429 + random.random()
                        print(f"[WARN] 429 on rotated, waiting {backoff:.1f}s (retry {attempt_429}/{MAX_RETRIES_429})")
                        time.sleep(backoff)
                        r = session.get(url, params=params, headers=headers, timeout=timeout)
                        if r.status_code != 429:
                            break
                return r
            except Exception as fallback_err:
                print(f"[WARN] Fallback failed: {fallback_err}")
            finally:
                session.proxies = old_proxies
            attempt += 1
            backoff = BACKOFF_BASE ** attempt + random.random()
            print(f"[WARN] Retry {attempt}/{MAX_RETRIES} in {backoff:.1f}s")
            time.sleep(backoff)
            continue
        except RequestException as e:
            attempt += 1
            backoff = BACKOFF_BASE ** attempt + random.random()
            print(f"[WARN] Request failed: {e}. Retry {attempt}/{MAX_RETRIES} in {backoff:.1f}s")
            time.sleep(backoff)
            continue
    return None

# ===================== ЗАГРУЗКА ПРЕДМЕТОВ =====================
def load_items(force_update: bool = False) -> Dict[str, Any]:
    if not os.path.exists(LOCAL_DB) or force_update:
        print("[INFO] Downloading items from ByMykel API...")
        r = session.get(BYMYKEL_URL, timeout=30)
        r.raise_for_status()
        items = r.json()
        try:
            with open(LOCAL_DB, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            print(f"[INFO] Saved {len(items)} items to {LOCAL_DB}")
        except Exception as e:
            print(f"[ERROR] Failed to save JSON: {e}")
            raise
    else:
        try:
            with open(LOCAL_DB, "r", encoding="utf-8") as f:
                items = json.load(f)
        except json.JSONDecodeError as e:
            print(f"[WARN] JSON error in {LOCAL_DB}: {e}. Reloading data.")
            return load_items(force_update=True)
    return items

def get_valid_items(items: dict) -> List[Dict[str, Any]]:
    return [it for it in items.values() if isinstance(it, dict) and it.get("name")]

def build_market_hash_name(item: dict) -> str:
    if item.get("market_hash_name"):
        return item["market_hash_name"]
    name = item.get("name", "").strip()
    if not name:
        return ""
    exterior = item.get("exterior")
    if exterior:
        name += f" ({exterior})"
    if item.get("stattrak"):
        name = f"StatTrak™ {name}"
    if item.get("souvenir"):
        name = f"Souvenir {name}"
    return name

def parse_order_table(soup: BeautifulSoup, table_id: str) -> List[List[float]]:
    div = soup.find("div", {"id": table_id})
    if not div:
        return []
    rows = div.find_all("tr")[1:]  # skip header
    price_qty = []
    for row in rows:
        tds = row.find_all("td")
        if len(tds) != 2:
            continue
        price_text = tds[0].text.strip()
        qty_text = tds[1].text.strip()
        # parse price
        if 'и ' in price_text:
            m = re.match(r'(.+?)( руб\. и)', price_text)
            if m:
                price_str = m.group(1).replace(',', '.').strip()
            else:
                price_str = price_text.replace(' руб.', '').replace(',', '.').strip()
        else:
            price_str = price_text.replace(' руб.', '').replace(',', '.').strip()
        try:
            price = float(price_str)
        except ValueError:
            continue
        try:
            qty = int(re.sub(r'[^\d]', '', qty_text))
        except ValueError:
            qty = 0
        if qty > 0:
            price_qty.append([price, qty])
    # sort asc by price
    price_qty.sort(key=lambda x: x[0])
    # compute cumulative
    graph = []
    cumul = 0
    for p, q in price_qty:
        cumul += q
        graph.append([p, float(cumul)])  # float for plot
    return graph

# ===================== ПОЛУЧЕНИЕ ИСТОРИИ ЦЕН И ДРУГИХ ДАННЫХ =====================
def get_item_data(market_hash_name: str) -> Dict[str, Any]:
    encoded = quote(market_hash_name, safe='')
    url = f"https://steamcommunity.com/market/listings/{APPID}/{encoded}"
    headers = {"Referer": url}
    r = request_with_retries(url, headers=headers, timeout=20)
    if not r or r.status_code != 200:
        print(f"[WARN] Failed to load page for {market_hash_name}")
        return {"history": [], "sell_listings": 0, "buy_orders": 0, "total_listings": 0, "price_usd": 0.0, "image_url": "", "histogram": None}

    soup = BeautifulSoup(r.text, "html.parser")

    # Парсинг истории цен
    scripts = soup.find_all("script")
    candidate = None
    for script in scripts:
        text = script.string
        if not text:
            continue
        m = re.search(r'var\s+line1\s*=\s*(\[\s*\[.*?\]\s*\])\s*;', text, re.DOTALL)
        if not m:
            m = re.search(r'var\s+g_rgHistory\s*=\s*(\[\s*\[.*?\]\s*\])\s*;', text, re.DOTALL)
        if not m:
            m2 = re.search(r'Market_LoadOrderHistogram\(\s*(\{.*?"sell_order_table".*?\})\s*\)', text, re.DOTALL)
            if m2:
                obj = safe_json_loads(m2.group(1))
                if obj:
                    pass
            m = None
        if m:
            candidate = m.group(1)
            break
    if candidate:
        parsed = safe_json_loads(candidate)
        if parsed:
            history = parsed
        else:
            arrs = re.findall(r'\[\s*([^\]]*?)\s*\]', candidate, re.DOTALL)
            history = []
            for arr in arrs:
                parts = [p.strip() for p in re.split(r'\s*,\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', arr)]
                if len(parts) >= 2:
                    date_part = parts[0].strip().strip('"').strip("'")
                    price_part = parts[1].strip().strip('"').strip("'")
                    vol_part = parts[2].strip().strip('"').strip("'") if len(parts) > 2 else "1"
                    vol_val = int(float(vol_part)) if vol_part else 1
                    history.append([date_part, price_part, vol_val])
    else:
        history = []

    # Парсинг total_listings
    total_listings = 0
    paging_summary = soup.find("div", class_="market_paging_summary ellipsis")
    if paging_summary:
        total_text = paging_summary.text.strip()
        total_match = re.search(r'из\s+(\d+)', total_text)
        if total_match:
            total_listings = int(total_match.group(1))
        else:
            total_span = soup.find("span", id="searchResults_total")
            if total_span:
                total_listings = int(total_span.text.strip())

    # Парсинг таблиц ордеров (основной метод)
    sell_graph = parse_order_table(soup, "market_commodity_forsale_table")
    buy_graph = parse_order_table(soup, "market_commodity_buyreqeusts_table")
    sell_listings = float(sell_graph[-1][1]) if sell_graph else 0
    buy_orders = float(buy_graph[-1][1]) if buy_graph else 0

    # Fallback к API, если таблицы пустые (только если item_nameid найден, чтобы не спамить)
    histogram = None
    if not sell_graph or not buy_graph:
        print(f"[INFO] Empty tables for {market_hash_name}, using API fallback")
        scripts = soup.find_all("script")
        item_nameid = None
        for script in scripts:
            text = script.string
            if text:
                m = re.search(r'Market_LoadOrderSpread\(\s*(\d+)\s*\)', text)
                if m:
                    item_nameid = m.group(1)
                    break
        if item_nameid:  # Только если нашли ID — запросим
            histogram_url = f"https://steamcommunity.com/market/itemordershistogram?country=RU&language=russian&currency=5&item_nameid={item_nameid}&two_factor=0&norender=1"
            r_hist = request_with_retries(histogram_url, timeout=20)
            if r_hist and r_hist.status_code == 200:
                j = r_hist.json()
                if 'success' in j and j['success'] == 1:
                    buy_orders = j.get('buy_order_count', buy_orders)
                    sell_listings = j.get('sell_order_count', sell_listings)
                    histogram = j
                    print(f"[INFO] API fallback: buy={buy_orders}, sell={sell_listings}")
                else:
                    print("[WARN] API success=0")
            else:
                print("[WARN] Failed API histogram")
    else:
        # Из таблиц
        all_prices = [p for p, c in sell_graph + buy_graph]
        all_cumuls = [c for p, c in sell_graph + buy_graph]
        min_x = min(all_prices) if all_prices else 0
        max_x = max(all_prices) if all_prices else 0
        max_y = max(all_cumuls) if all_cumuls else 0
        histogram = {
            "buy_order_graph": buy_graph,
            "sell_order_graph": sell_graph,
            "graph_min_x": min_x,
            "graph_max_x": max_x,
            "graph_max_y": max_y
        }

    # Парсинг lowest price
    price_span = soup.find("span", class_="market_listing_price market_listing_price_with_fee")
    price_usd = 0.0
    if price_span:
        price_text = price_span.text.strip()
        usd_match = re.search(r'\$([\d.]+)', price_text)
        if usd_match:
            price_usd = float(usd_match.group(1))
        else:
            price_usd = parse_price_text(price_text)

    # Парсинг image_url
    image_elem = soup.find("img", id="largeItemImage")
    image_url = image_elem['src'] if image_elem and image_elem.get('src') else ""

    return {
        "history": history,
        "sell_listings": sell_listings,
        "buy_orders": buy_orders,
        "total_listings": total_listings,
        "price_usd": price_usd,
        "image_url": image_url,
        "histogram": histogram
    }

# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================
def parse_price_text(s):
    if isinstance(s, (int, float)):
        return float(s)
    if not isinstance(s, str):
        return 0.0
    s = s.replace("$", "").replace("USD", "").replace("\xa0", "").replace("руб.", "").replace("₽", "").replace(",", ".").replace(" ", "")
    try:
        return float(s)
    except ValueError:
        return 0.0

def format_usd(price: float) -> str:
    if price == 0:
        return "$0.00"
    return f"${price:.2f}"

def format_rub(price: float) -> str:
    if price == 0:
        return "0,00₽"
    int_part = f"{int(price):,}".replace(",", " ")
    dec_part = f"{price:.2f}".split(".")[1]
    return f"{int_part},{dec_part}₽"

def parse_volume(s):
    if isinstance(s, int):
        return s
    if not isinstance(s, str):
        return 0
    s = s.replace(",", "").replace(" ", "").replace("\xa0", "")
    try:
        return int(s)
    except ValueError:
        return 0

def df_from_pricehistory(prices_raw, usd_rate: float = USD_RATE):
    rows = []
    now = datetime.now(tz=TZ)
    cutoff_date = now - timedelta(days=HISTORY_DAYS)
    skipped_old = 0
    for i in range(len(prices_raw) - 1, -1, -1):
        p = prices_raw[i]
        try:
            date_raw, price_str, volume_str = p
            date_raw = re.sub(r' \+\d+$', '', date_raw).strip()
            if ':' in date_raw:
                date_raw = date_raw.rstrip(':').strip()
            parts = date_raw.split()
            if len(parts) >= 4 and len(parts[-1]) == 2:
                date_raw = ' '.join(parts[:-1]) + ' ' + parts[-1] + ':00'
            price = parse_price_text(price_str)
            volume = parse_volume(volume_str)
            dt = pd.to_datetime(date_raw, utc=True, dayfirst=False, errors='coerce')
            if pd.isna(dt):
                continue
            dt = dt.tz_convert('Europe/Moscow')
            if dt < cutoff_date:
                skipped_old += 1
                break
            rows.append({"timestamp": dt, "price_usd": price, "volume": volume})
        except Exception:
            continue
    rows.reverse()
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp").reset_index(drop=True)
        df["price_rub"] = df["price_usd"] * usd_rate
    return df

def analyze_dataframe(df: pd.DataFrame, current_median: float, current_volume: int):
    if df.empty or len(df) < 2:
        return {
            "volatility": 0.0,
            "price_growth": 0.0,
            "volume_growth": 0.0,
            "breakout_percentage": 0.0,
            "range_breakout": 0.0,
            "is_sideways": False,
            "range_percent": 0.0
        }
    prices = df["price_usd"].values
    avg_price = float(prices.mean())
    stdev_price = float(prices.std(ddof=0))
    volatility = (stdev_price / avg_price * 100) if avg_price > 0 else 0.0
    now = datetime.now(tz=TZ)
    last_24h_start = now - timedelta(hours=24)
    prev_24h_start = now - timedelta(hours=48)
    prev_24h_end = last_24h_start
    last_24h = df[df["timestamp"] >= last_24h_start]
    prev_24h = df[(df["timestamp"] >= prev_24h_start) & (df["timestamp"] < prev_24h_end)]
    prev_price = prev_24h["price_usd"].mean() if not prev_24h.empty else avg_price
    price_growth = ((current_median - prev_price) / prev_price * 100) if prev_price > 0 else 0.0
    prev_volume = prev_24h["volume"].sum() if not prev_24h.empty else 0
    volume_growth = 0.0
    if prev_volume > 0:
        volume_growth = ((current_volume - prev_volume) / prev_volume * 100)
    elif prev_volume == 0 and current_volume > 0:
        volume_growth = 100.0
    elif current_volume == 0:
        volume_growth = -100.0
    week_start = now - timedelta(days=HISTORY_DAYS)
    week_df = df[df["timestamp"] >= week_start]
    max_price_week = week_df["price_usd"].max() if not week_df.empty else 0.0
    min_price_week = week_df["price_usd"].min() if not week_df.empty else 0.0
    breakout_percentage = ((current_median - max_price_week) / max_price_week * 100) if max_price_week > 0 else 0.0
    range_percent = ((max_price_week - min_price_week) / min_price_week * 100) if min_price_week > 0 else 0.0
    is_sideways = range_percent < 20.0
    range_breakout = 0.0
    if is_sideways and min_price_week > 0:
        upper_bound = max_price_week * 1.10
        lower_bound = min_price_week * 0.90
        if current_median > upper_bound:
            range_breakout = ((current_median - max_price_week) / max_price_week * 100)
        elif current_median < lower_bound:
            range_breakout = ((current_median - min_price_week) / min_price_week * 100) * -1
    return {
        "volatility": round(volatility, 2),
        "price_growth": round(price_growth, 2),
        "volume_growth": round(volume_growth, 2),
        "breakout_percentage": round(breakout_percentage, 2),
        "range_breakout": round(abs(range_breakout), 2),
        "is_sideways": is_sideways,
        "range_percent": round(range_percent, 2)
    }

def item_passes_criteria(item: dict) -> tuple[bool, str]:
    if item["price_usd"] < MIN_PRICE:
        return False, f"price < {MIN_PRICE}"
    if item["volume_24h"] < MIN_VOLUME_24H:
        return False, f"volume < {MIN_VOLUME_24H}"
    if item.get("is_sideways", False) and item.get("range_breakout", 0) >= 10.0:
        return True, "range breakout from sideways"
    if item.get("breakout_percentage", 0) >= BREAKOUT_THRESHOLD:
        return True, "breakout threshold"
    if item.get("volatility", 0) > VOLATILITY_THRESHOLD or abs(item.get("growth", 0)) >= PRICE_CHANGE_THRESHOLD:
        return True, "volatility or price change"
    return False, "no criteria met"

def create_empty_buf():
    buf = BytesIO()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.set_facecolor('#1b2838')
    fig.patch.set_facecolor('#1b2838')
    ax.grid(True, linestyle='--', alpha=0.2, color='#555')
    ax.tick_params(axis='x', colors='#ccc', labelrotation=45)
    ax.tick_params(axis='y', colors='#ccc')
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight", facecolor='#1b2838')
    plt.close(fig)
    buf.seek(0)
    return buf

def russian_month_formatter(x, pos):
    dt = mdates.num2date(x)
    months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
    return f"{dt.day:02d} {months[dt.month - 1]}"

def plot_price_week(df: pd.DataFrame, title: str):
    if df.empty:
        return create_empty_buf()
    now = datetime.now(tz=TZ)
    week_df = df[df["timestamp"] >= (now - timedelta(days=HISTORY_DAYS))]
    if week_df.empty:
        week_df = df.copy()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(week_df["timestamp"], week_df["price_rub"], linestyle='-', color='#00a1d6', linewidth=1.5)
    ax.set_title(title, fontsize=10, color='#fff', pad=10)
    ax.set_xlabel("Дата", fontsize=8, color='#ccc')
    ax.set_ylabel("Цена (₽)", fontsize=8, color='#ccc')
    ax.grid(True, linestyle='--', alpha=0.2, color='#555')
    ax.tick_params(axis='x', colors='#ccc', labelrotation=45)
    ax.tick_params(axis='y', colors='#ccc')
    ax.xaxis.set_major_formatter(FuncFormatter(russian_month_formatter))
    fig.patch.set_facecolor('#1b2838')
    ax.set_facecolor('#1b2838')
    plt.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, facecolor='#1b2838', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return buf

def plot_volume_week(df: pd.DataFrame, title: str):
    if df.empty:
        return create_empty_buf()
    now = datetime.now(tz=TZ)
    week_df = df[df["timestamp"] >= (now - timedelta(days=HISTORY_DAYS))]
    if week_df.empty:
        week_df = df.copy()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(week_df["timestamp"], week_df["volume"], linestyle='-', color='#00a1d6', linewidth=1.5)
    ax.set_title(title, fontsize=10, color='#fff', pad=10)
    ax.set_xlabel("Дата", fontsize=8, color='#ccc')
    ax.set_ylabel("Объём", fontsize=8, color='#ccc')
    ax.grid(True, linestyle='--', alpha=0.2, color='#555')
    ax.tick_params(axis='x', colors='#ccc', labelrotation=45)
    ax.tick_params(axis='y', colors='#ccc')
    ax.xaxis.set_major_formatter(FuncFormatter(russian_month_formatter))
    fig.patch.set_facecolor('#1b2838')
    ax.set_facecolor('#1b2838')
    plt.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, facecolor='#1b2838', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return buf

def build_plots(item: dict, days: int = HISTORY_DAYS) -> tuple[BytesIO, BytesIO, BytesIO]:
    df = df_from_pricehistory(item.get("history_raw", []), item.get("usd_rate", USD_RATE))
    price_buf = plot_price_week(df, f"Динамика цены за {days} дней — {item['name']}")
    volume_buf = plot_volume_week(df, f"Объём продаж за {days} дней — {item['name']}")
    histogram = item.get("histogram")
    if not histogram or not histogram.get("buy_order_graph") or not histogram.get("sell_order_graph"):
        order_buf = create_empty_buf()
    else:
        fig_order, ax_order = plt.subplots(figsize=(10, 4))
        fig_order.patch.set_facecolor('#1b2838')
        ax_order.set_facecolor('#1b2838')
        ax_order.set_title(f"Книга ордеров — {item['name']}", fontsize=10, color='#fff', pad=10)
        
        # Buy orders (зелёный, asc prices, increasing cumul)
        buy_graph = histogram["buy_order_graph"]
        buy_prices = [row[0] for row in buy_graph]
        buy_cumuls = [row[1] for row in buy_graph]
        ax_order.fill_between(buy_prices, 0, buy_cumuls, step='post', color='#00FF00', alpha=0.5)
        ax_order.plot(buy_prices, buy_cumuls, color='#00FF00', drawstyle='steps-post', linewidth=1.5, label='Запросов на покупку (Зелёный)')
        
        # Sell orders (красный, asc prices, increasing cumul)
        sell_graph = histogram["sell_order_graph"]
        sell_prices = [row[0] for row in sell_graph]
        sell_cumuls = [row[1] for row in sell_graph]
        ax_order.fill_between(sell_prices, 0, sell_cumuls, step='post', color='#FF0000', alpha=0.5)
        ax_order.plot(sell_prices, sell_cumuls, color='#FF0000', drawstyle='steps-post', linewidth=1.5, label='Лотов на продажу (Красный)')
        
        ax_order.set_ylabel("Количество", fontsize=8, color='#ccc')
        ax_order.set_xlabel("Цена (₽)", fontsize=8, color='#ccc')
        ax_order.grid(True, linestyle="--", alpha=0.2, color='#555')
        ax_order.tick_params(colors='#ccc')
        ax_order.tick_params(axis='x', labelrotation=45)
        if "graph_min_x" in histogram and "graph_max_x" in histogram:
            ax_order.set_xlim(histogram["graph_min_x"], histogram["graph_max_x"])
        if "graph_max_y" in histogram:
            ax_order.set_ylim(0, histogram["graph_max_y"] * 1.2)
        ax_order.legend(loc='upper left', fontsize=8, facecolor='#1b2838', edgecolor='#ccc', labelcolor='#ccc')
        order_buf = BytesIO()
        fig_order.savefig(order_buf, format="png", dpi=120, facecolor='#1b2838', edgecolor='none')
        plt.close(fig_order)
        order_buf.seek(0)
    return price_buf, volume_buf, order_buf

def send_media_group_telegram(media_files, caption=""):
    send_url = f"https://api.telegram.org/bot{TOKEN}/sendMediaGroup"
    files = {}
    media_list = []
    for i, (file_type, file_data) in enumerate(media_files):
        if file_type == 'photo':
            files[f"photo{i}"] = file_data
            media_list.append({"type": "photo", "media": f"attach://photo{i}"})
    if media_list:
        media_list[0]["caption"] = caption
        media_list[0]["parse_mode"] = "HTML"
    payload = {"chat_id": CHAT_ID, "media": json.dumps(media_list)}
    try:
        r = session.post(send_url, data=payload, files=files, timeout=20)
        if r.status_code == 200:
            j = r.json()
            if j.get("ok"):
                print("[INFO] Telegram media group sent")
                return True
            else:
                print(f"[WARN] Telegram media OK=False: {j}")
                return False
        else:
            print(f"[WARN] Telegram media HTTP {r.status_code}: {r.text[:100]}...")
            return False
    except Exception as e:
        print(f"[ERROR] Telegram media error: {e}")
        return False

def send_message_telegram(text: str) -> bool:
    send_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": "true"}
    try:
        r = session.post(send_url, data=payload, timeout=12)
        if r.status_code == 200 and r.json().get("ok"):
            print("[INFO] Telegram message sent")
            return True
        else:
            print(f"[WARN] Telegram message failed: {r.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] Telegram message error: {e}")
        return False

# ===================== ЛОГ ПОСТОВ =====================
def load_posted_log() -> List[str]:
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            print(f"[WARN] Failed to load {LOG_FILE}, starting empty")
            return []
    return []

def save_posted_log(log: List[str]):
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)

# ===================== ИТОГИ ДНЯ =====================
def generate_daily_summary(items_analyzed: List[Dict], posted_log: List[str]):
    # Фильтруем только предметы, которые были опубликованы
    posted_items = [item for item in items_analyzed if item.get("market_hash_name") in posted_log]
    
    top_growth = sorted(
        [item for item in posted_items if item.get("growth", 0) > 0],
        key=lambda x: x.get("growth", 0),
        reverse=True
    )[:5]
    top_decline = sorted(
        [item for item in posted_items if item.get("growth", 0) < 0],
        key=lambda x: x.get("growth", 0)
    )[:5]

    # Формирование текста и загрузка изображений для топа роста
    summary_growth = "🕐 ИТОГИ за 24 часа:\n\n🟢 ТОП-5 предметов по росту:\n\n"
    growth_media = []
    for i, item in enumerate(top_growth, 1):
        steam_url = f"https://steamcommunity.com/market/listings/{APPID}/{quote(item['market_hash_name'], safe='')}"
        summary_growth += (
            f"{i}️⃣ <a href=\"{steam_url}\">{html.escape(item['name'])}</a>\n"
            f"   ﹂Цена: {format_rub(item['price_rub'])} | Волатильность: {item['volatility']:.2f}% | Изменение цены: +{item['growth']:.2f}%\n\n"
        )
        if item.get("image_url"):
            r_img = request_with_retries(item["image_url"], timeout=10, force_direct=True)
            if r_img and r_img.status_code == 200:
                growth_media.append(('photo', r_img.content))
                print(f"[INFO] Loaded image for summary growth: {item['market_hash_name']}")
            else:
                print(f"[WARN] Failed image for summary growth: {item['market_hash_name']}")
    if not top_growth:
        summary_growth += "Нет предметов с ростом цены за последние 24 часа.\n"

    # Формирование текста и загрузка изображений для топа падения
    summary_decline = "🔴 ТОП-5 предметов по падению:\n\n"
    decline_media = []
    for i, item in enumerate(top_decline, 1):
        steam_url = f"https://steamcommunity.com/market/listings/{APPID}/{quote(item['market_hash_name'], safe='')}"
        summary_decline += (
            f"{i}️⃣ <a href=\"{steam_url}\">{html.escape(item['name'])}</a>\n"
            f"   ﹂Цена: {format_rub(item['price_rub'])} | Волатильность: {item['volatility']:.2f}% | Изменение цены: {item['growth']:.2f}%\n\n"
        )
        if item.get("image_url"):
            r_img = request_with_retries(item["image_url"], timeout=10, force_direct=True)
            if r_img and r_img.status_code == 200:
                decline_media.append(('photo', r_img.content))
                print(f"[INFO] Loaded image for summary decline: {item['market_hash_name']}")
            else:
                print(f"[WARN] Failed image for summary decline: {item['market_hash_name']}")
    if not top_decline:
        summary_decline += "Нет предметов с падением цены за последние 24 часа.\n"

    return (summary_growth, growth_media), (summary_decline, decline_media)

# ===================== MAIN =====================
def main():
    global USD_RATE, USE_PROXY
    parser = argparse.ArgumentParser(description="CSGO Market Analyzer")
    parser.add_argument('--send-summary', action='store_true', help="Send daily summary of top growth and decline items")
    parser.add_argument('--summary-time', type=str, default=DEFAULT_SUMMARY_TIME, help="Time to send summary in HH:MM format (EEST), e.g., '16:39'")
    args = parser.parse_args()

    # Обновляем курс USD/RUB динамически
    USD_RATE = get_usd_to_rub_rate()

    # Проверка формата времени
    try:
        summary_time = datetime.strptime(args.summary_time, "%H:%M").time()
    except ValueError:
        print(f"[WARN] Invalid time format '{args.summary_time}'. Using default: {DEFAULT_SUMMARY_TIME}")
        summary_time = datetime.strptime(DEFAULT_SUMMARY_TIME, "%H:%M").time()

    # Тест прокси
    if USE_PROXY:
        try:
            r = session.get("https://api.ipify.org?format=json", timeout=8)
            ip = r.json().get("ip") if r.status_code == 200 else None
            print(f"[INFO] Proxy IP: {ip}")
            r2 = session.get("https://steamcommunity.com", timeout=8)
            print(f"[INFO] Steam status via proxy: {r2.status_code}")
        except Exception as e:
            print(f"[WARN] Proxy test failed: {e}. Switching to direct")
            USE_PROXY = False
            disable_proxy()
    else:
        disable_proxy()

    items_raw = load_items()
    valid_items = get_valid_items(items_raw)
    print(f"[INFO] Loaded {len(valid_items)} valid items")

    posted_log = load_posted_log()
    items_analyzed = []

    while True:  # Бесконечный цикл
        # Перемешиваем список предметов для случайного порядка
        shuffled_items = random.sample(valid_items, len(valid_items))
        print(f"[INFO] Starting scan cycle: {len(shuffled_items)} items")

        for item in shuffled_items:
            try:
                # Проверка времени для отправки итогов
                now_eest = datetime.now(tz=EEST_TZ)
                target_time = now_eest.replace(hour=summary_time.hour, minute=summary_time.minute, second=0, microsecond=0)
                time_diff = abs((now_eest - target_time).total_seconds())
                
                last_summary = load_last_summary()
                summary_sent_in_24h = last_summary and (now_eest - last_summary).total_seconds() < 24 * 3600
                
                send_summary = (args.send_summary or time_diff <= 60) and not summary_sent_in_24h  # Окно ±1 мин

                if send_summary and items_analyzed:
                    print("[INFO] Generating and sending 24h summary")
                    (summary_growth, growth_media), (summary_decline, decline_media) = generate_daily_summary(items_analyzed, posted_log)
                    
                    # Отправка топа роста
                    if growth_media:
                        sent_growth = send_media_group_telegram(growth_media, summary_growth)
                        if not sent_growth:
                            sent_growth = send_message_telegram(summary_growth)
                    else:
                        sent_growth = send_message_telegram(summary_growth)
                    
                    time.sleep(2)
                    
                    # Отправка топа падения
                    if decline_media:
                        sent_decline = send_media_group_telegram(decline_media, summary_decline)
                        if not sent_decline:
                            sent_decline = send_message_telegram(summary_decline)
                    else:
                        sent_decline = send_message_telegram(summary_decline)
                    
                    # Если обе сводки отправлены успешно, очищаем всё
                    if sent_growth and sent_decline:
                        print("[INFO] Full cleanup after summary sent")
                        # Очистка лога опубликованных
                        posted_log = []
                        save_posted_log(posted_log)
                        # Очистка списка проанализированных
                        items_analyzed = []
                        # Сохранение времени отправки сводки
                        save_last_summary(now_eest)
                        # Очистка папки с CSV
                        if os.path.exists(OUT_DIR):
                            shutil.rmtree(OUT_DIR, ignore_errors=True)
                        os.makedirs(OUT_DIR, exist_ok=True)
                        print("[INFO] Cleanup done: logs, analyzed, OUT_DIR")
                    else:
                        print("[WARN] Partial summary send, no cleanup")

                    # Задержка после отправки итогов
                    time.sleep(REQUEST_DELAY + random.random() * JITTER)

                mhn = build_market_hash_name(item)
                if not mhn:
                    continue
                item["market_hash_name"] = mhn

                if mhn in posted_log:
                    print(f"[INFO] Skipping posted: {mhn}")
                    continue

                print(f"[INFO] Loading data: {mhn}")
                time.sleep(REQUEST_DELAY + random.random() * JITTER)
                data = get_item_data(mhn)
                raw_history = data["history"]
                if not raw_history:
                    print(f"[WARN] No history for {mhn}")
                    continue

                df = df_from_pricehistory(raw_history, USD_RATE)
                if df.empty:
                    print(f"[WARN] Empty DF for {mhn}")
                    continue

                if not df.empty:
                    current_price_usd = df.iloc[-1]["price_usd"]
                    now = datetime.now(tz=TZ)
                    last_24h_start = now - timedelta(hours=24)
                    volume_24h = int(df[df["timestamp"] >= last_24h_start]["volume"].sum())
                else:
                    current_price_usd = 0.0
                    volume_24h = 0

                item["price_usd"] = data["price_usd"] if data["price_usd"] > 0 else current_price_usd
                item["price_rub"] = item["price_usd"] * USD_RATE
                item["volume_24h"] = volume_24h
                item["history_raw"] = raw_history  # Для build_plots
                item["histogram"] = data.get("histogram")  # Для графика ордеров
                item["usd_rate"] = USD_RATE  # Передаём в item для графиков

                fallback_image = item.get('image', '')
                if fallback_image.startswith('http'):
                    item["image_url"] = fallback_image
                else:
                    item["image_url"] = f"https://community.cloud.steamstatic.com/economy/image/{fallback_image}/360fx360f"
                if data["image_url"]:
                    item["image_url"] = data["image_url"]

                item["name"] = item.get("name", mhn)
                item["publications"] = 1
                item["sell_listings"] = data["sell_listings"]
                item["buy_orders"] = data["buy_orders"]
                item["total_listings"] = data["total_listings"]

                analysis = analyze_dataframe(df, current_median=item["price_usd"], current_volume=volume_24h)
                item["volatility"] = analysis["volatility"]
                item["growth"] = analysis["price_growth"]
                item["volume_change"] = analysis["volume_growth"]
                item["breakout_percentage"] = analysis["breakout_percentage"]
                item["is_sideways"] = analysis["is_sideways"]
                item["range_breakout"] = analysis["range_breakout"]
                item["range_percent"] = analysis["range_percent"]

                print(f"[INFO] {mhn}: vol_24h={item['volume_24h']}, growth={item['growth']:.1f}%, vol={item['volatility']:.1f}% | sells={item['sell_listings']}, buys={item['buy_orders']}")

                passed, reason = item_passes_criteria(item)
                if not passed:
                    print(f"[INFO] {mhn} skipped: {reason}")
                    items_analyzed.append(item)
                    continue

                print(f"[INFO] {mhn} passes criteria: {reason}")
                items_analyzed.append(item)

                safe_name = re.sub(r"[^\w\-_.() ]", "_", mhn)[:120]
                csv_name = os.path.join(OUT_DIR, f"prices_{safe_name}.csv")
                df.to_csv(csv_name, index=False, encoding="utf-8")
                print(f"[INFO] Saved CSV: {csv_name}")

                # Построение всех трёх графиков
                price_buf, volume_buf, order_buf = build_plots(item, HISTORY_DAYS)

                steam_url = f"https://steamcommunity.com/market/listings/{APPID}/{quote(mhn, safe='')}"
                growth_sign = "+" if item["growth"] >= 0 else ""
                volume_sign = "+" if item["volume_change"] >= 0 else ""
                color_emoji = "🟢" if item["growth"] >= 0 else "🔴"
                caption = (
                    f"<a href=\"{steam_url}\">{html.escape(item['name'])}</a>\n\n"
                    f"{color_emoji} Стоимость: {format_rub(item['price_rub'])} ({format_usd(item['price_usd'])}) (24 часа: {growth_sign}{item['growth']:.2f}%)\n"
                    f"🔘 Объем продаж: {item['volume_24h']} (24 часа: {volume_sign}{item['volume_change']:.2f}%)\n"
                    f"🔘 Волатильность: {item['volatility']:.2f}%\n\n"
                    f"📤 Лотов на продажу: {item['sell_listings']}\n"
                    f"📥 Запросов на покупку: {item['buy_orders']}\n\n"
                    f"🏆 Публикаций за сутки в канал: {item['publications']}"
                )

                media_files = []
                if item["image_url"]:
                    r_img = request_with_retries(item["image_url"], timeout=10, force_direct=True)
                    if r_img and r_img.status_code == 200:
                        media_files.append(('photo', r_img.content))
                        print(f"[INFO] Loaded item image: {mhn}")
                    else:
                        print(f"[WARN] Failed item image: {mhn}")

                media_files.append(('photo', price_buf.getvalue()))
                media_files.append(('photo', volume_buf.getvalue()))
                media_files.append(('photo', order_buf.getvalue()))  # График ордеров

                sent = send_media_group_telegram(media_files, caption)
                
                if sent:
                    print(f"[INFO] Posted media: {mhn}")
                    posted_log.append(mhn)
                    save_posted_log(posted_log)
                else:
                    print(f"[WARN] Media failed for {mhn}, trying text")
                    sent_text = send_message_telegram(caption)
                    if sent_text:
                        print(f"[INFO] Posted text: {mhn}")
                        posted_log.append(mhn)
                        save_posted_log(posted_log)
                    else:
                        print(f"[ERROR] Failed to post {mhn}")

            except Exception as e:
                print(f"[ERROR] Unexpected error for {mhn if 'mhn' in locals() else 'unknown'}: {e}")
                time.sleep(REQUEST_DELAY + random.random() * JITTER)
                continue

if __name__ == "__main__":
    main()