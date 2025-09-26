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
import argparse

# ===================== НАСТРОЙКИ =====================
BYMYKEL_URL = "https://bymykel.github.io/CSGO-API/api/en/all.json"
LOCAL_DB = "items.json"
APPID = 730

# Telegram
TOKEN = os.environ.get("TELEGRAM_TOKEN", "7524644623:AAE6YasVXvYnnNH-xrbSH_odIHEqD_b15oo")
CHAT_ID = os.environ.get("CHAT_ID", "-1002695033602")

# Steam sessionid
SESSIONID = os.environ.get("STEAM_SESSIONID", None)

# Прокси
USE_PROXY_BY_DEFAULT = True
PROXY_HTTP_URL = "http://mm4pkP:a6K4yx@95.181.155.167:8000"
PROXY_HTTP_ALT = "http://lte6:LVxqnyQiMH@65.109.79.15:13014"

# Поведение запросов
REQUEST_DELAY = 9.0
JITTER = 1.0
MAX_RETRIES = 3
MAX_RETRIES_429 = 4
BACKOFF_BASE = 2.0

# Фильтры
VOLATILITY_THRESHOLD = 9.0
PRICE_CHANGE_THRESHOLD = 8.0
BREAKOUT_THRESHOLD = 2.0
MIN_PRICE = 3.0
MIN_VOLUME_24H = 1
HISTORY_DAYS = 7
USD_RATE = 83.4

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
    session.proxies.update({"http": proxy_url, "https": proxy_url})
    print("[proxy] включен:", proxy_url)

def disable_proxy():
    session.proxies.clear()
    print("[proxy] отключён (прямое соединение)")

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

# ===================== РЕКВЕСТЫ С RETRY / FALLBACK =====================
def request_with_retries(url: str, params=None, headers=None, timeout=15, allow_429_backoff=True, force_direct=False):
    if force_direct:
        session.proxies.clear()
        print("[request] Force direct connection for this request")
    
    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429 and allow_429_backoff:
                attempt_429 = 0
                while attempt_429 < MAX_RETRIES_429:
                    attempt_429 += 1
                    backoff = BACKOFF_BASE ** attempt_429 + random.random()
                    print(f"[request] 429 — ждём {backoff:.1f}s (retry429 {attempt_429}/{MAX_RETRIES_429})")
                    time.sleep(backoff)
                    r = session.get(url, params=params, headers=headers, timeout=timeout)
                    if r.status_code != 429:
                        break
            return r
        except (SSLError, ProxyError) as err:
            print(f"[request] {type(err).__name__}: {err}. Попытка fallback: отключаем прокси и пробуем напрямую.")
            old_proxies = session.proxies.copy()
            try:
                session.proxies.clear()
                try:
                    r = session.get(url, params=params, headers=headers, timeout=timeout)
                    if r.status_code == 429 and allow_429_backoff:
                        attempt_429 = 0
                        while attempt_429 < MAX_RETRIES_429:
                            attempt_429 += 1
                            backoff = BACKOFF_BASE ** attempt_429 + random.random()
                            print(f"[request] 429 (direct) — ждём {backoff:.1f}s (retry429 {attempt_429}/{MAX_RETRIES_429})")
                            time.sleep(backoff)
                            r = session.get(url, params=params, headers=headers, timeout=timeout)
                            if r.status_code != 429:
                                break
                    return r
                except SSLError as ssl2:
                    print(f"[request] Direct SSLError: {ssl2}")
                    if ALLOW_INSECURE:
                        print("[request] ALLOW_INSECURE=True -> пробуем verify=False (unsafe!)")
                        try:
                            r = session.get(url, params=params, headers=headers, timeout=timeout, verify=False)
                            return r
                        except Exception as e:
                            print(f"[request] verify=False также упал: {e}")
                except RequestException as dr:
                    print(f"[request] Direct request exception: {dr}")
            finally:
                session.proxies = old_proxies
            attempt += 1
            backoff = BACKOFF_BASE ** attempt + random.random()
            print(f"[request] После ошибки: retry {attempt}/{MAX_RETRIES} через {backoff:.1f}s")
            time.sleep(backoff)
            continue
        except RequestException as e:
            attempt += 1
            backoff = BACKOFF_BASE ** attempt + random.random()
            print(f"[request] RequestException: {e}. retry {attempt}/{MAX_RETRIES} через {backoff:.1f}s")
            time.sleep(backoff)
            continue
    return None

# ===================== ЗАГРУЗКА ПРЕДМЕТОВ =====================
def load_items(force_update: bool = False) -> Dict[str, Any]:
    if not os.path.exists(LOCAL_DB) or force_update:
        print("Скачиваем предметы с ByMykel API...")
        r = session.get(BYMYKEL_URL, timeout=30)
        r.raise_for_status()
        items = r.json()
        try:
            with open(LOCAL_DB, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            print(f"Сохранено {len(items)} предметов в {LOCAL_DB}")
        except Exception as e:
            print(f"[load_items] Ошибка при сохранении JSON: {e}")
            raise
    else:
        try:
            with open(LOCAL_DB, "r", encoding="utf-8") as f:
                items = json.load(f)
        except json.JSONDecodeError as e:
            print(f"[load_items] Ошибка в JSON: {e}. Загружаем свежие данные.")
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

# ===================== ПОЛУЧЕНИЕ ИСТОРИИ ЦЕН И ДРУГИХ ДАННЫХ =====================
def get_item_data(market_hash_name: str) -> Dict[str, Any]:
    encoded = quote(market_hash_name, safe='')
    url = f"https://steamcommunity.com/market/listings/{APPID}/{encoded}"
    headers = {"Referer": url}
    r = request_with_retries(url, headers=headers, timeout=20)
    if not r or r.status_code != 200:
        print(f"[item_data] Не удалось загрузить страницу для {market_hash_name}")
        return {"history": [], "sell_listings": 0, "buy_orders": 0, "total_listings": 0, "price_usd": 0.0, "image_url": ""}

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

    # Парсинг buy_orders
    buy_orders = 0
    buy_requests_div = soup.find("div", id="market_commodity_buyrequests")
    if buy_requests_div:
        promote_span = buy_requests_div.find("span", class_="market_commodity_orders_header_promote")
        if promote_span:
            buy_text = promote_span.text.strip().replace(',', '')
            buy_match = re.search(r'(\d+)', buy_text)
            if buy_match:
                buy_orders = int(buy_match.group(1))
                print(f"[parse] Запросов на покупку: {buy_orders}")
            else:
                print("[parse] Не удалось извлечь число из promote_span:", promote_span.text)
        else:
            print("[parse] Не найден promote_span в market_commodity_buyrequests")
            buy_text_match = re.search(r'Запросов на покупку:\s*(\d+)', buy_requests_div.text)
            if buy_text_match:
                buy_orders = int(buy_text_match.group(1))
                print(f"[parse] Запросов на покупку (альтернативный метод): {buy_orders}")
            else:
                print("[parse] Не удалось найти Запросов на покупку в тексте div")
    else:
        print("[parse] Не найден div market_commodity_buyrequests")

    # Парсинг sell_listings
    sell_listings = total_listings
    orders_header = soup.find("div", id="market_commodity_orders_header")
    if orders_header:
        promote_spans = orders_header.find_all("span", class_=re.compile(".*promote.*"))
        if len(promote_spans) >= 2:
            try:
                sell_text = promote_spans[1].text.strip().replace(',', '')
                sell_match = re.search(r'(\d+)', sell_text)
                if sell_match:
                    sell_listings = int(sell_match.group(1))
            except ValueError:
                print("[parse] Ошибка парсинга чисел sell")
        else:
            sell_text = soup.find(text=re.compile(r'sell listings?', re.I))
            if sell_text:
                sell_match = re.search(r'(\d+)', str(sell_text.parent))
                if sell_match:
                    sell_listings = int(sell_match.group(1))
    else:
        print("[parse] Не найден orders_header div для sell_listings")
        for script in scripts:
            text = script.string
            if text:
                m_hist = re.search(r'var\s+g_rgOrderHistogram\s*=\s*(\[.*?\]);', text, re.DOTALL)
                if m_hist:
                    hist = safe_json_loads(m_hist.group(1))
                    if hist:
                        if len(hist) >= 2:
                            buy_orders = sum(hist[0]) if isinstance(hist[0], list) else hist[0]
                            sell_listings = sum(hist[1]) if isinstance(hist[1], list) else hist[1]
                            print(f"[parse] Из orders_histogram: buy={buy_orders}, sell={sell_listings}")
                            break

    # Альтернативный метод через histogram
    scripts = soup.find_all("script")
    item_nameid = None
    for script in scripts:
        text = script.string
        if text:
            m = re.search(r'Market_LoadOrderSpread\(\s*(\d+)\s*\)', text)
            if m:
                item_nameid = m.group(1)
                break

    if item_nameid:
        histogram_url = f"https://steamcommunity.com/market/itemordershistogram?country=RU&language=russian&currency=5&item_nameid={item_nameid}&two_factor=0&norender=1"
        r_hist = request_with_retries(histogram_url, timeout=20)
        if r_hist and r_hist.status_code == 200:
            j = r_hist.json()
            if 'success' in j and j['success'] == 1:
                buy_orders = j.get('buy_order_count', 0)
                sell_listings = j.get('sell_order_count', 0)
                print(f"[parse] From histogram: buy={buy_orders}, sell={sell_listings}")
            else:
                print("[parse] Histogram JSON success=0")
        else:
            print("[parse] Failed to load histogram")
    else:
        print("[parse] item_nameid not found")

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
        "image_url": image_url
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

def df_from_pricehistory(prices_raw):
    print("[df_from_pricehistory] Начало обработки истории цен: всего записей", len(prices_raw))
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
                print("[df_from_pricehistory] Неверная дата в записи:", p)
                continue
            dt = dt.tz_convert('Europe/Moscow')
            if dt < cutoff_date:
                skipped_old += 1
                break
            rows.append({"timestamp": dt, "price_usd": price, "volume": volume})
        except Exception as e:
            print("[df_from_pricehistory] Ошибка разбора записи истории:", p, "-", e)
            continue
    rows.reverse()
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("timestamp").reset_index(drop=True)
        df["price_rub"] = df["price_usd"] * USD_RATE
    print("[df_from_pricehistory] Сформирован DataFrame с", len(df), "строками (пропущено", skipped_old, "старых записей)")
    return df

def analyze_dataframe(df: pd.DataFrame, current_median: float, current_volume: int):
    print("[analyze_dataframe] Начало анализа DataFrame: строк=", len(df), ", median=", current_median, ", volume=", current_volume)
    if df.empty or len(df) < 2:
        print("[analyze_dataframe] DataFrame пустой или слишком мал для анализа. Возвращаю нулевые значения.")
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
    print("[analyze_dataframe] Анализ: volatility=", volatility, ", price_growth=", price_growth, ", volume_growth=", volume_growth, ", breakout=", breakout_percentage, ", range_breakout=", range_breakout)
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
    reason = ""
    if item["price_usd"] < MIN_PRICE:
        reason = f"price < {MIN_PRICE} ({item['price_usd']})"
        print("[criteria] Не проходит:", reason)
        return False, reason
    if item["volume_24h"] < MIN_VOLUME_24H:
        reason = f"volume_24h < {MIN_VOLUME_24H} ({item['volume_24h']})"
        print("[criteria] Не проходит:", reason)
        return False, reason
    if item.get("is_sideways", False) and item.get("range_breakout", 0) >= 10.0:
        reason = f"range_breakout={item['range_breakout']}% >=10% из боковика (range={item.get('range_percent', 0):.1f}%)"
        print("[criteria] Проходит:", reason)
        return True, reason
    if item.get("breakout_percentage", 0) >= BREAKOUT_THRESHOLD:
        reason = f"breakout_percentage={item['breakout_percentage']} >= {BREAKOUT_THRESHOLD}"
        print("[criteria] Проходит:", reason)
        return True, reason
    if item.get("volatility", 0) > VOLATILITY_THRESHOLD or abs(item.get("growth", 0)) >= PRICE_CHANGE_THRESHOLD:
        reason = f"volatility={item['volatility']} > {VOLATILITY_THRESHOLD} or |growth|={abs(item['growth'])} >= {PRICE_CHANGE_THRESHOLD}"
        print("[criteria] Проходит:", reason)
        return True, reason
    reason = f"volatility={item.get('volatility', 0)} <= {VOLATILITY_THRESHOLD} and |growth|={abs(item.get('growth', 0))} < {PRICE_CHANGE_THRESHOLD} and breakout={item.get('breakout_percentage', 0)} < {BREAKOUT_THRESHOLD} and range_breakout={item.get('range_breakout', 0)}<10%"
    print("[criteria] Не проходит:", reason)
    return False, reason

def create_empty_buf():
    buf = io.BytesIO()
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
    buf = io.BytesIO()
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
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, facecolor='#1b2838', edgecolor='none')
    plt.close(fig)
    buf.seek(0)
    return buf

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
                print("[telegram/media_group] Отправлено")
                return True
            else:
                print("[telegram/media_group] OK=False:", j)
                return False
        else:
            print("[telegram/media_group] HTTP", r.status_code, ":", r.text)
            return False
    except Exception as e:
        print("[telegram/media_group] Ошибка:", e)
        return False

def send_message_telegram(text: str) -> bool:
    send_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": "true"}
    try:
        r = session.post(send_url, data=payload, timeout=12)
        return r.status_code == 200 and r.json().get("ok")
    except Exception as e:
        print(f"[telegram] Error: {e}")
        return False

# ===================== ЛОГ ПОСТОВ =====================
def load_posted_log() -> List[str]:
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
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
                print(f"[summary] Загружено изображение для {item['market_hash_name']} (рост)")
            else:
                print(f"[summary] Не удалось загрузить изображение для {item['market_hash_name']}: {item['image_url']}")
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
                print(f"[summary] Загружено изображение для {item['market_hash_name']} (падение)")
            else:
                print(f"[summary] Не удалось загрузить изображение для {item['market_hash_name']}: {item['image_url']}")
    if not top_decline:
        summary_decline += "Нет предметов с падением цены за последние 24 часа.\n"

    return (summary_growth, growth_media), (summary_decline, decline_media)

# ===================== MAIN =====================
def main():
    global USE_PROXY
    parser = argparse.ArgumentParser(description="CSGO Market Analyzer")
    parser.add_argument('--send-summary', action='store_true', help="Send daily summary of top growth and decline items")
    parser.add_argument('--summary-time', type=str, default=DEFAULT_SUMMARY_TIME, help="Time to send summary in HH:MM format (EEST), e.g., '16:39'")
    args = parser.parse_args()

    # Проверка формата времени
    try:
        summary_time = datetime.strptime(args.summary_time, "%H:%M").time()
    except ValueError:
        print(f"[main] Ошибка: неверный формат времени '{args.summary_time}'. Используется время по умолчанию: {DEFAULT_SUMMARY_TIME}")
        summary_time = datetime.strptime(DEFAULT_SUMMARY_TIME, "%H:%M").time()

    # Тест прокси
    if USE_PROXY:
        try:
            r = session.get("https://api.ipify.org?format=json", timeout=8)
            ip = r.json().get("ip") if r.status_code == 200 else None
            print("[proxy] external IP:", ip)
            r2 = session.get("https://steamcommunity.com", timeout=8)
            print("[steam] main status via proxy:", r2.status_code)
        except Exception as e:
            print("[proxy] проверка через прокси не удалась:", e)
            print("[proxy] переключаем на прямое соединение")
            USE_PROXY = False
            disable_proxy()
    else:
        disable_proxy()

    items_raw = load_items()
    valid_items = get_valid_items(items_raw)
    print(f"[main] Загружено {len(valid_items)} валидных предметов.")

    posted_log = load_posted_log()
    items_analyzed = []

    while True:  # Бесконечный цикл
        # Перемешиваем список предметов для случайного порядка
        shuffled_items = random.sample(valid_items, len(valid_items))
        print(f"[main] Начинаем новый цикл сканирования: {len(shuffled_items)} предметов.")

        for item in shuffled_items:
            try:
                # Проверка времени для отправки итогов
                now_eest = datetime.now(tz=EEST_TZ)
                target_time = now_eest.replace(hour=summary_time.hour, minute=summary_time.minute, second=0, microsecond=0)
                time_diff = abs((now_eest - target_time).total_seconds())
                send_summary = args.send_summary or time_diff <= 300  # Окно ±5 минут

                if send_summary and items_analyzed:
                    print("[main] Генерация и отправка итогов за 24 часа")
                    (summary_growth, growth_media), (summary_decline, decline_media) = generate_daily_summary(items_analyzed, posted_log)
                    
                    # Отправка топа роста
                    if growth_media:
                        sent_growth = send_media_group_telegram(growth_media, summary_growth)
                        if sent_growth:
                            print("[main] Итоги по росту отправлены с изображениями")
                        else:
                            print("[main] Не удалось отправить итоги по росту с изображениями, пробуем текст")
                            sent_growth = send_message_telegram(summary_growth)
                            if sent_growth:
                                print("[main] Итоги по росту отправлены как текст")
                            else:
                                print("[main] Не удалось отправить итоги по росту")
                    else:
                        sent_growth = send_message_telegram(summary_growth)
                        if sent_growth:
                            print("[main] Итоги по росту отправлены как текст (нет изображений)")
                        else:
                            print("[main] Не удалось отправить итоги по росту")
                    
                    time.sleep(2)
                    
                    # Отправка топа падения
                    if decline_media:
                        sent_decline = send_media_group_telegram(decline_media, summary_decline)
                        if sent_decline:
                            print("[main] Итоги по падению отправлены с изображениями")
                        else:
                            print("[main] Не удалось отправить итоги по падению с изображениями, пробуем текст")
                            sent_decline = send_message_telegram(summary_decline)
                            if sent_decline:
                                print("[main] Итоги по падению отправлены как текст")
                            else:
                                print("[main] Не удалось отправить итоги по падению")
                    else:
                        sent_decline = send_message_telegram(summary_decline)
                        if sent_decline:
                            print("[main] Итоги по падению отправлены как текст (нет изображений)")
                        else:
                            print("[main] Не удалось отправить итоги по падению")
                    
                    # Очистка лога и списка после успешной отправки итогов
                    if sent_growth and sent_decline:
                        print("[main] Очистка лога опубликованных предметов и списка проанализированных")
                        posted_log = []
                        items_analyzed = []
                        save_posted_log(posted_log)
                    else:
                        print("[main] Лог не очищен, так как не все итоги были отправлены")

                    # Задержка после отправки итогов
                    time.sleep(REQUEST_DELAY + random.random() * JITTER)

                mhn = build_market_hash_name(item)
                if not mhn:
                    continue
                item["market_hash_name"] = mhn

                if mhn in posted_log:
                    print(f"[main] Пропускаем (уже опубликовано в этом цикле): {mhn}")
                    continue

                print(f"\n[main] Загружаем данные: {mhn}")
                time.sleep(REQUEST_DELAY + random.random() * JITTER)
                data = get_item_data(mhn)
                raw_history = data["history"]
                if not raw_history:
                    print(f"[main] Нет истории для {mhn}")
                    continue

                df = df_from_pricehistory(raw_history)
                if df.empty:
                    print(f"[main] Не удалось создать DF для {mhn}")
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

                print(f"[main] Метрики для {mhn}: объем_24h={item['volume_24h']}, волатильность={item['volatility']:.2f}, рост={item['growth']:.2f}")
                print(f"[main] Дополнительно: лотов на продажу={item['sell_listings']}, запросов на покупку={item['buy_orders']}, всего лотов={item['total_listings']}")

                passed, reason = item_passes_criteria(item)
                if not passed:
                    print(f"[main] Не прошёл критерии: {mhn} ({reason})")
                    items_analyzed.append(item)
                    continue

                items_analyzed.append(item)

                safe_name = re.sub(r"[^\w\-_.() ]", "_", mhn)[:120]
                csv_name = os.path.join(OUT_DIR, f"prices_{safe_name}.csv")
                df.to_csv(csv_name, index=False, encoding="utf-8")
                print(f"[main] Сохранён CSV: {csv_name}")

                price_buf = plot_price_week(df, f"Динамика цены за {HISTORY_DAYS} дней — {item['name']}")
                volume_buf = plot_volume_week(df, f"Объём продаж за {HISTORY_DAYS} дней — {item['name']}")

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
                        print(f"[image] Загружено фото для {mhn}")
                    else:
                        print(f"[image] Не удалось загрузить фото: {item['image_url']}")
                media_files.append(('photo', price_buf.getvalue()))
                media_files.append(('photo', volume_buf.getvalue()))

                sent = send_media_group_telegram(media_files, caption)
                
                if sent:
                    print(f"[main] Успешно запощен: {mhn}")
                    posted_log.append(mhn)
                    save_posted_log(posted_log)
                else:
                    print("[main] Отправка медиа не удалась — fallback на текст")
                    sent_text = send_message_telegram(caption)
                    if sent_text:
                        print(f"[main] Успешно запощен (текст): {mhn}")
                        posted_log.append(mhn)
                        save_posted_log(posted_log)

            except Exception as e:
                print(f"[main] Неожиданная ошибка при обработке предмета {mhn}: {e}")
                time.sleep(REQUEST_DELAY + random.random() * JITTER)
                continue

if __name__ == "__main__":
    main()