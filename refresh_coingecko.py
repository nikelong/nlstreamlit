"""
Оновлення локального кешу списку крипто-токенів з CoinGecko.

Запускається через GitHub Actions (refresh-coingecko.yml) вручну.
Тягне https://api.coingecko.com/api/v3/coins/list (~17,500 токенів, без ключа)
і зберігає у data/coingecko_cache.json.

Після запуску — data/coingecko_cache.json комітиться у main workflow'ом.
fetch.py читає цей файл локально через categorize.load_coingecko_cache().
"""
import os
import json
import time
from datetime import datetime, timezone
import requests

OUTPUT_PATH = "data/coingecko_cache.json"
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/list"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def fetch_coingecko_list() -> list[dict]:
    """
    Тягне /coins/list з retry (до 5 разів) з експоненційним backoff.
    Повертає список токенів [{id, symbol, name}, ...].
    """
    for attempt in range(1, 6):
        try:
            print(f"🔵 Спроба {attempt}/5: GET {COINGECKO_URL}")
            r = requests.get(COINGECKO_URL, headers=HEADERS, timeout=30)
            if r.status_code == 200 and r.text.strip().startswith("["):
                coins = r.json()
                print(f"✅ Отримано {len(coins):,} токенів")
                return coins
            print(f"⚠️ Status {r.status_code}, body: {r.text[:200]}")
        except Exception as e:
            print(f"❌ Помилка: {type(e).__name__}: {e}")
        if attempt < 5:
            wait = 2 ** attempt
            print(f"   ⏳ Чекаю {wait}s перед наступною спробою...")
            time.sleep(wait)
    raise RuntimeError(f"Не вдалось отримати {COINGECKO_URL} за 5 спроб")


def save_cache(coins: list[dict], path: str = OUTPUT_PATH) -> None:
    """
    Зберігає кеш у JSON з метаданими.
    Формат: {"updated_at": "ISO8601", "source": "...", "count": N, "coins": [...]}
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":     COINGECKO_URL,
        "count":      len(coins),
        "coins":      [
            {"id": c.get("id", ""), "symbol": c.get("symbol", ""), "name": c.get("name", "")}
            for c in coins
        ],
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    size_kb = os.path.getsize(path) / 1024
    print(f"💾 Збережено {path} ({size_kb:,.0f} KB)")


def print_sample(coins: list[dict]) -> None:
    """Друкує кілька прикладів для sanity check."""
    samples = ["btc", "eth", "sol", "hype", "fartcoin"]
    found = {c["symbol"].lower(): c for c in coins if c.get("symbol", "").lower() in samples}
    print("🧪 Sanity check — приклади знайдених токенів:")
    for sym in samples:
        if sym in found:
            c = found[sym]
            print(f"   ✅ {sym.upper():10s} = {c['name']:40s} (id={c['id']})")
        else:
            print(f"   ⚠️ {sym.upper():10s} — НЕ знайдено у списку")


if __name__ == "__main__":
    print("═" * 60)
    print(f"🚀 Оновлення CoinGecko cache — {datetime.now(timezone.utc).isoformat()}")
    print("═" * 60)

    t0 = time.time()
    try:
        coins = fetch_coingecko_list()
        save_cache(coins)
        print_sample(coins)
        print("═" * 60)
        print(f"✅ Готово за {time.time()-t0:.1f}s")
        print("═" * 60)
    except Exception as e:
        print("═" * 60)
        print(f"❌ ФАТАЛЬНА ПОМИЛКА: {type(e).__name__}: {e}")
        print("═" * 60)
        raise
