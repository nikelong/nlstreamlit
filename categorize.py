"""
Категоризація тікерів perp DEX у 5 класів активів + канонізація base.

4 шари обробки:
  A. canonical_base(ticker, base_hint) — нормалізація формату
  B. 4 TRADFI_* словники — hardcoded маппінг TradFi
  C. coingecko_cache — fallback для Crypto через локальний JSON
  D. USER_OVERRIDES — ручні правки після manual audit

Головна точка входу: apply(df, exchange_name, category_hint_col=None)
— додає 6 колонок: Base, Canonical Base, Asset Name, Category, HIP-3 Dex, Quote Currency
"""
import re
import json
import os
from pathlib import Path
import pandas as pd

# ---------------------------------------------------------------------------
# Константи
# ---------------------------------------------------------------------------

COINGECKO_CACHE_PATH = "data/coingecko_cache.json"
CATEGORIES = ["Crypto", "Stocks", "Commodities", "FX", "Indices"]

# Емоджі категорій — для verbose логів і UI
CATEGORY_EMOJI = {
    "Crypto":      "🟦",
    "Stocks":      "🟧",
    "Commodities": "🟫",
    "FX":          "🟨",
    "Indices":     "🟪",
}

# Кольори категорій (hex) — для Streamlit Plotly
CATEGORY_COLOR = {
    "Crypto":      "#3B82F6",
    "Stocks":      "#F97316",
    "Commodities": "#92400E",
    "FX":          "#EAB308",
    "Indices":     "#A855F7",
}

# ---------------------------------------------------------------------------
# Шар A — нормалізація base (чиста текстова функція, без даних)
# ---------------------------------------------------------------------------

_QUOTE_SUFFIXES = ("USDC", "USDT", "USD", "BUSD")
_PERP_SUFFIXES  = ("-PERP", "_Perp", "_PERP")
_DELIMITERS = "/-_"

def _strip_hip3_prefix(ticker: str) -> tuple[str, str | None]:
    """
    Витягує HIP-3 dex-префікс з тікеру Hyperliquid.

    Формати у parquet:
      'xyz:xyz:CL/USDC'  → ('CL/USDC', 'xyz')       ← реальний формат з API
      'xyz:CL/USDC'       → ('CL/USDC', 'xyz')       ← скорочений
      'BTC/USDC'          → ('BTC/USDC', None)       ← native
    """
    if ":" not in ticker:
        return ticker, None
    parts = ticker.split(":")
    dex = parts[0]
    # Якщо той самий префікс повторюється двічі (xyz:xyz:CL/USDC) — прибираємо обидва
    rest = ":".join(parts[1:])
    if rest.startswith(dex + ":"):
        rest = rest[len(dex) + 1:]
    return rest, dex


def _extract_quote(ticker: str, base: str) -> str:
    """Виділяє quote currency з тікеру (USDC/USDT/USD/USDT1 etc.)."""
    t = ticker.upper()
    # 1) Явний розділювач: "BTC/USDC", "BTC-USD-PERP", "BTC_USDT_PERP"
    for delim in _DELIMITERS:
        if delim in t:
            after = t.split(delim, 1)[1]
            for suffix in ("USDC", "USDT", "USD", "BUSD"):
                if after.startswith(suffix):
                    return suffix
    # 2) Приклеєний суфікс: "BTCUSDT" → "USDT"
    for suffix in ("USDC", "USDT", "USD", "BUSD"):
        if t.endswith(suffix) and len(t) > len(suffix):
            return suffix
    return "USD"  # за замовчуванням


def _raw_base(ticker: str, base_hint: str = "") -> str:
    """
    Витягує сирий base з тікеру.
    Використовує base_hint з API, якщо він є (пріоритет), інакше — парсить ticker.
    """
    if base_hint:
        return base_hint.upper()

    t = ticker

    # Extended _24_5 суфікс: "AAPL_24_5-USD" → "AAPL"
    t = re.sub(r"_\d+_\d+-?", "-", t)

    # PERP-суфікси: "BTC-USD-PERP", "BTC_USDT_Perp"
    for suffix in _PERP_SUFFIXES:
        if t.endswith(suffix):
            t = t[:-len(suffix)]

    # Розділювач → беремо першу частину
    for delim in _DELIMITERS:
        if delim in t:
            t = t.split(delim)[0]
            break

    t = t.upper().rstrip("-_")

    # Aster "BTCUSD1", "ETHUSD2" — знімаємо цифровий суфікс ПІСЛЯ USD
    # Це специфічно Aster: USD1/USD2 — варіанти однієї пари
    # Не зачіпає SP500/XYZ100/MAG7 — в них цифри НЕ після USD
    t = re.sub(r"USD[\d]+$", "USD", t)

    # Приклеєний quote-суфікс: "BTCUSDT" → "BTC"
    for suffix in ("USDC", "USDT", "USD", "BUSD"):
        if t.endswith(suffix) and len(t) > len(suffix):
            t = t[:-len(suffix)]
            break

    return t


def deeper_canonical(b: str) -> str:
    """
    Другий шар нормалізації: k-prefix, 1000-prefix, tokenization aliases.
    Застосовується після _raw_base().
    """
    if not b:
        return ""

    # 1000-prefix: "1000PEPE" → "PEPE", "1000000MOG" → "MOG"
    m = re.match(r"^1[0]+([A-Z][A-Z0-9]*)$", b)
    if m:
        return m.group(1)

    # k-prefix: "kPEPE" → "PEPE", "KBONK" → "BONK"
    if re.match(r"^[kK][A-Z][A-Z0-9]+$", b) and len(b) >= 4:
        return b[1:]

    # USD-суфікс БЕЗ сепаратора (Lighter грабелька #12): "HYUNDAIUSD" → "HYUNDAI"
    # Тільки якщо залишок — коректний тікер і довший за 2 символи
    if b.endswith("USD") and len(b) > 5:
        without = b[:-3]
        if re.match(r"^[A-Z][A-Z0-9]*$", without) and len(without) >= 3:
            return without

    return b


def canonical_base(ticker: str, base_hint: str = "") -> tuple[str, str, str, str | None]:
    """
    Головна функція Шару A.

    Повертає tuple: (base, canonical, quote, hip3_dex)
      base       — сирий base з тікеру (напр., "BTC", "BTCUSD1" → "BTC")
      canonical  — після deeper_canonical (напр., "kPEPE" → "PEPE")
      quote      — котирувальна валюта (USDC/USDT/USD)
      hip3_dex   — HIP-3 dex-префікс для Hyperliquid, або None
    """
    # 1) HIP-3 префікс
    clean_ticker, hip3_dex = _strip_hip3_prefix(ticker)

    # 2) Raw base
    base = _raw_base(clean_ticker, base_hint)

    # 3) Canonical
    canon = deeper_canonical(base)

    # 4) Quote
    quote = _extract_quote(clean_ticker, base)

    return base, canon, quote, hip3_dex


# ---------------------------------------------------------------------------
# Шар B — TradFi словники (hardcoded, Ваш кастом)
# ---------------------------------------------------------------------------

TRADFI_STOCKS = {
    # === US Tech mega-cap ===
    "AAPL": "Apple Inc.", "MSFT": "Microsoft", "GOOG": "Alphabet (Class C)",
    "GOOGL": "Alphabet (Class A)", "AMZN": "Amazon.com", "META": "Meta Platforms",
    "NVDA": "NVIDIA", "AMD": "Advanced Micro Devices", "INTC": "Intel", "ORCL": "Oracle",
    "TSLA": "Tesla", "NFLX": "Netflix", "ADBE": "Adobe", "CRM": "Salesforce",
    "CSCO": "Cisco", "IBM": "IBM", "QCOM": "Qualcomm", "TXN": "Texas Instruments",
    "AVGO": "Broadcom", "MU": "Micron", "WDC": "Western Digital",
    "SANDISK": "SanDisk", "SNDK": "SanDisk",
    "PLTR": "Palantir Technologies", "RIVN": "Rivian Automotive",
    "RTX": "RTX Corporation", "LLY": "Eli Lilly", "GME": "GameStop",
    "COST": "Costco", "BX": "Blackstone", "DKNG": "DraftKings",
    "HIMS": "Hims & Hers Health", "CRWV": "CoreWeave", "BIRD": "Allbirds",

    # === US Crypto-related ===
    "COIN": "Coinbase Global", "HOOD": "Robinhood Markets",
    "CRCL": "Circle Internet Group", "MSTR": "MicroStrategy",
    "BMNR": "BitMine Immersion", "SBET": "SharpLink Gaming",

    # === US Finance / Industrial / Consumer / Health ===
    "JPM": "JPMorgan Chase", "BAC": "Bank of America", "C": "Citigroup",
    "V": "Visa", "MA": "Mastercard", "F": "Ford Motor", "GM": "General Motors",
    "CVX": "Chevron", "XOM": "Exxon Mobil", "T": "AT&T", "VZ": "Verizon",
    "WMT": "Walmart", "JNJ": "Johnson & Johnson", "PFE": "Pfizer",
    "PAYP": "PayPal Holdings",

    # === Asian stocks ===
    "TSM": "Taiwan Semiconductor (TSMC)", "TSMC": "TSMC",
    "SAMSUNG": "Samsung Electronics", "SMSN": "Samsung (alt)",
    "HYUNDAI": "Hyundai Motor", "LG": "LG Electronics",
    "SK": "SK Hynix", "SKHYNIX": "SK Hynix", "SKHX": "SK Hynix (alt)",
    "KIA": "Kia", "POSCO": "POSCO Holdings",
    "SONY": "Sony Group", "TOYOTA": "Toyota Motor", "HONDA": "Honda Motor",
    "NISSAN": "Nissan Motor", "SOFTBANK": "SoftBank Group",
    "NINTENDO": "Nintendo", "KEYENCE": "Keyence", "KIOXIA": "Kioxia",
    "HITACHI": "Hitachi", "CANON": "Canon", "PANASONIC": "Panasonic",
    "FOXCONN": "Hon Hai (Foxconn)", "MEDIATEK": "MediaTek",
    "RELIANCE": "Reliance Industries", "INFY": "Infosys",
    "TCS": "Tata Consultancy", "HANMI": "Hanmi Semiconductor",
    "BABA": "Alibaba Group", "JD": "JD.com", "PDD": "PDD Holdings",
    "TENCENT": "Tencent Holdings", "XIAOMI": "Xiaomi",
    "NIO": "NIO", "XPEV": "XPeng", "LI": "Li Auto",

    # === European stocks ===
    "ASML": "ASML Holding", "SAP": "SAP SE", "SIEMENS": "Siemens",
    "BMW": "BMW", "MBG": "Mercedes-Benz", "VW": "Volkswagen",
    "BAYN": "Bayer", "BASF": "BASF", "ADIDAS": "Adidas", "PUMA": "Puma",
    "AIRBUS": "Airbus", "SAFRAN": "Safran", "TOTAL": "TotalEnergies",
    "LVMH": "LVMH", "HERMES": "Hermès", "LOREAL": "L'Oréal",
    "KERING": "Kering", "SANOFI": "Sanofi", "NESTLE": "Nestlé",
    "NOVARTIS": "Novartis", "ROCHE": "Roche", "UBS": "UBS Group",
    "SHELL": "Shell plc", "BP": "BP plc", "HSBC": "HSBC Holdings",
    "BARC": "Barclays", "AZN": "AstraZeneca", "GSK": "GlaxoSmithKline",
    "ULVR": "Unilever", "RIO": "Rio Tinto", "BHP": "BHP",
    "VOD": "Vodafone", "ING": "ING Group", "PHIA": "Philips",
    "SPOTIFY": "Spotify",

    # === Country stock ETFs (кошики акцій, НЕ індекси) ===
    "EWY": "South Korea Stock ETF", "EWJ": "Japan Stock ETF",
    "EWZ": "Brazil Stock ETF", "FXI": "China Large-Cap ETF",
    "EWG": "Germany ETF", "EWU": "UK ETF", "INDA": "India ETF",
    "KWEB": "China Internet ETF",

    # === Thematic stock ETFs ===
    "ARKK": "ARK Innovation ETF", "ARKG": "ARK Genomic ETF",

    # === xStocks / tokenized stocks ===
    "STRC": "Strategy PP Variable xStock",

    # === HIP-3 private/semi-private ===
    "OPENAI": "OpenAI (private, Ventuals)",
    "SPACEX": "SpaceX (private, Ventuals)",
    "ANTHROPIC": "Anthropic (Ventuals private)",
    "USAR": "USA Rare Earth", "WEB": "Webull",
}

TRADFI_COMMODITIES = {
    # Oil & Energy
    "BZ": "Brent", "BRENT": "Brent", "BRENTOIL": "Brent", "XBR": "Brent (alt)",
    "CL": "WTI Crude Oil", "WTI": "WTI Crude Oil", "WTIOIL": "WTI Crude Oil",
    "USOIL": "WTI (alt)", "OIL": "Crude Oil", "USO": "US Oil Fund (ETF)",
    "NG": "Natural Gas", "NATGAS": "Natural Gas", "XNG": "Natural Gas (alt)",
    "GAS": "Natural Gas (alt)",

    # Precious metals
    "GOLD": "Gold", "XAU": "Gold", "XAUT": "Tether Gold",
    "PAXG": "PAX Gold (tokenized)",
    "GLD": "Gold ETF (SPDR)", "IAU": "iShares Gold Trust ETF",
    "SILVER": "Silver", "XAG": "Silver", "SLV": "Silver ETF (iShares)",
    "PLATINUM": "Platinum", "XPT": "Platinum",
    "PALLADIUM": "Palladium", "XPD": "Palladium",

    # Industrial metals
    "COPPER": "Copper", "XCU": "Copper",
    "ALUMINIUM": "Aluminium", "ALUMINUM": "Aluminium",

    # Agriculture
    "CC": "Cocoa", "COCOA": "Cocoa", "COFFEE": "Coffee", "SUGAR": "Sugar",
    "WHEAT": "Wheat", "CORN": "Corn", "SOYBEAN": "Soybean",

    # Uranium (commodity exposure ETFs)
    "URA": "Uranium ETF", "URNM": "Uranium Miners ETF",
}

TRADFI_FX = {
    # Major currencies (as base)
    "EUR": "Euro", "GBP": "British Pound", "JPY": "Japanese Yen",
    "CHF": "Swiss Franc", "CAD": "Canadian Dollar", "AUD": "Australian Dollar",
    "NZD": "New Zealand Dollar", "CNY": "Chinese Yuan", "CNH": "CNH",
    "HKD": "Hong Kong Dollar", "KRW": "Korean Won", "SGD": "Singapore Dollar",
    "INR": "Indian Rupee", "BRL": "Brazilian Real", "MXN": "Mexican Peso",
    "ZAR": "South African Rand", "TRY": "Turkish Lira",

    # USD index
    "DXY": "USD Dollar Index",

    # Pairs (якщо збереглись у такому вигляді)
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "AUDUSD": "AUD/USD",
    "NZDUSD": "NZD/USD", "USDJPY": "USD/JPY", "USDCHF": "USD/CHF",
    "USDCAD": "USD/CAD", "USDKRW": "USD/KRW", "USDCNY": "USD/CNY",
    "USDINR": "USD/INR", "USDBRL": "USD/BRL", "USDTRY": "USD/TRY",
    "EURJPY": "EUR/JPY", "EURGBP": "EUR/GBP", "GBPJPY": "GBP/JPY",
}

TRADFI_INDICES = {
    # === US broad-market + broad-market ETFs ===
    "SPX": "S&P 500 Index", "SP500": "S&P 500", "SPX500": "S&P 500",
    "USA500": "S&P 500", "US500": "S&P 500",
    "SPX500m": "S&P 500 (Extended mini)", "SPY": "S&P 500 ETF (SPDR)",
    "NDX": "Nasdaq-100", "NAS100": "Nasdaq-100", "TECH100": "Nasdaq-100",
    "USTECH": "US Tech Index", "USA100": "US Top 100 Index",
    "TECH100m": "Nasdaq-100 (Extended mini)", "QQQ": "Nasdaq-100 ETF (Invesco)",
    "DJI": "Dow Jones", "DOW": "Dow Jones", "US30": "Dow Jones (US30)",
    "DIA": "Dow Jones ETF",
    "RUT": "Russell 2000", "US2000": "Russell 2000", "SMALL2000": "Russell 2000 (alt)",
    "IWM": "Russell 2000 ETF",
    "VIX": "CBOE Volatility Index",

    # === European indices ===
    "DAX": "DAX 40", "DAX40": "DAX 40", "FTSE": "FTSE 100", "UK100": "FTSE 100",
    "CAC": "CAC 40", "CAC40": "CAC 40",
    "STOXX": "Euro Stoxx 50", "SX5E": "Euro Stoxx 50",

    # === Asian indices ===
    "N225": "Nikkei 225", "NIKKEI": "Nikkei 225", "JP225": "Nikkei 225",
    "HSI": "Hang Seng", "KOSPI": "KOSPI Index",
    "KS200": "KOSPI 200", "KR200": "KOSPI 200",
    "KRCOMP": "KOSPI Composite", "ASX": "ASX 200", "TPX": "Topix",
    "SENSEX": "BSE Sensex", "NIFTY": "NIFTY 50",

    # === HIP-3 specific indices ===
    "MAG7": "Magnificent 7 Index", "XYZ100": "XYZ100 Index (Trade.XYZ)",
    "USENERGY": "US Energy Sector Index", "ENERGY": "Energy Sector Index",
    "DEFENSE": "Defense Sector Index", "NUCLEAR": "Nuclear Sector Index",
    "SEMIS": "Semiconductor Index", "SEMI": "Semiconductor Index",
    "BIOTECH": "Biotech Sector Index", "INFOTECH": "Info Tech Sector Index",
    "GLDMINE": "Gold Miners Index", "USBOND": "US Bond Index",
}

# Швидкий lookup: canonical_base → (category, asset_name)
_TRADFI_LOOKUP = {}
for canon, name in TRADFI_STOCKS.items():      _TRADFI_LOOKUP[canon.upper()] = ("Stocks", name)
for canon, name in TRADFI_COMMODITIES.items(): _TRADFI_LOOKUP[canon.upper()] = ("Commodities", name)
for canon, name in TRADFI_FX.items():          _TRADFI_LOOKUP[canon.upper()] = ("FX", name)
for canon, name in TRADFI_INDICES.items():     _TRADFI_LOOKUP[canon.upper()] = ("Indices", name)


# ---------------------------------------------------------------------------
# Шар C — CoinGecko cache (локальний JSON, оновлюється окремим workflow)
# ---------------------------------------------------------------------------

_coingecko_map: dict[str, str] | None = None


def load_coingecko_cache(path: str = COINGECKO_CACHE_PATH) -> dict[str, str]:
    """
    Читає data/coingecko_cache.json і повертає мапу symbol (upper) → name.
    Якщо файла немає — повертає пустий dict (тоді Crypto буде без asset_name).
    Кешується у памʼяті — зчитується один раз за процес.
    """
    global _coingecko_map
    if _coingecko_map is not None:
        return _coingecko_map

    if not os.path.exists(path):
        print(f"⚠️ CoinGecko cache не знайдено: {path}")
        print(f"   Запустіть workflow 'Refresh CoinGecko cache' для створення.")
        _coingecko_map = {}
        return _coingecko_map

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        coins = data.get("coins", [])
        _coingecko_map = {c["symbol"].upper(): c["name"] for c in coins if c.get("symbol")}
        updated = data.get("updated_at", "невідомо")
        print(f"✅ CoinGecko cache: {len(_coingecko_map)} токенів, оновлено {updated}")
    except Exception as e:
        print(f"❌ CoinGecko cache пошкоджено: {e}")
        _coingecko_map = {}

    return _coingecko_map


# ---------------------------------------------------------------------------
# Шар D — USER_OVERRIDES (ручні правки після manual audit)
# ---------------------------------------------------------------------------

USER_OVERRIDES: dict[str, tuple[str, str]] = {
    # TradFi знайдені вручну (якщо не спрацював USD-суфікс Шару A)
    "HYUNDAIUSD":  ("Stocks", "Hyundai Motor"),
    "SAMSUNGUSD":  ("Stocks", "Samsung Electronics"),
    "SKHYNIXUSD":  ("Stocks", "SK Hynix"),

    # Crypto v2-контракти EdgeX (нульовий volume, але для чистоти категорій)
    "LUNA2":       ("Crypto", "Terra (v2)"),
    "TIA2":        ("Crypto", "Tiamonds (v2, NOT Celestia)"),
    "VIRTUAL2":    ("Crypto", "Virtuals Protocol (v2)"),
    "BNB2":        ("Crypto", "BNB (v2)"),
    "LTC2":        ("Crypto", "Litecoin (v2)"),
    "ETH2":        ("Crypto", "Ethereum (v2)"),
    "SOL2":        ("Crypto", "Solana (v2)"),

    # Crypto без точної назви у CoinGecko або з конфліктними тікерами
    "LIT":         ("Crypto", "Lighter (LIT)"),
    "LIGHTER":     ("Crypto", "Lighter"),
    "LAUNCHCOIN":  ("Crypto", "Launch Coin"),
    "PUMPFUN":     ("Crypto", "Pump.fun"),
    "FARTCOIN":    ("Crypto", "Fartcoin"),
    "HPOS10I":     ("Crypto", "HarryPotterObamaSonic10Inu"),
    "EDGE":        ("Crypto", "edgeX Token"),
    "GENIUS":      ("Crypto", "Genius"),
    "RAVE":        ("Crypto", "RaveDAO"),
    "ASTER":       ("Crypto", "Aster"),
    "HYPE":        ("Crypto", "Hyperliquid"),

    # HIP-3 Crypto (Ventuals тощо)
    "ROBOT":       ("Crypto", "Robot (Ventuals)"),
}

# Нормалізуємо ключі в USER_OVERRIDES до upper case
USER_OVERRIDES = {k.upper(): v for k, v in USER_OVERRIDES.items()}


# ---------------------------------------------------------------------------
# Core: categorize — поєднує всі 4 шари
# ---------------------------------------------------------------------------

def categorize(canonical: str, category_hint: str = "") -> tuple[str, str]:
    """
    Повертає (category, asset_name) для canonical base.

    Порядок перевірки:
      1. USER_OVERRIDES — найвищий пріоритет (ручні правки виграють завжди)
      2. TRADFI словники — якщо це відомий TradFi актив
      3. category_hint з API (наприклад, Extended віддає 'Crypto'/'Stocks') — fallback
      4. CoinGecko cache — якщо є в мапі крипто-символів
      5. Без-asset_name Crypto — дефолт якщо нічого не знайдено
    """
    if not canonical:
        return ("Unknown", "")

    canon_up = canonical.upper()

    # 1) USER_OVERRIDES — виграють усе
    if canon_up in USER_OVERRIDES:
        return USER_OVERRIDES[canon_up]

    # 2) TradFi словники
    if canon_up in _TRADFI_LOOKUP:
        return _TRADFI_LOOKUP[canon_up]

    # 3) CoinGecko cache
    cg = load_coingecko_cache()
    if canon_up in cg:
        return ("Crypto", cg[canon_up])

    # 4) Category hint з API (Extended має поле category)
    if category_hint:
        hint = category_hint.strip().capitalize()
        if hint in CATEGORIES:
            return (hint, "")  # asset_name лишиться порожнім

    # 5) Дефолт — Crypto без імені
    return ("Crypto", "")


# ---------------------------------------------------------------------------
# Головна точка входу — apply(df) для fetch.py
# ---------------------------------------------------------------------------

def apply(
    df: pd.DataFrame,
    exchange_name: str,
    base_col: str = None,
    category_hint_col: str = None,
) -> pd.DataFrame:
    """
    Додає 6 колонок до DataFrame з тікерами:
      Base              — сирий base з тікеру
      Canonical Base    — після deeper_canonical
      Asset Name        — читабельна назва (Bitcoin, Apple Inc., WTI Crude Oil)
      Category          — Crypto / Stocks / Commodities / FX / Indices
      HIP-3 Dex         — dex-префікс для Hyperliquid (xyz, cash, ...) або None
      Quote Currency    — USDC / USDT / USD

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame з обовʼязковою колонкою 'Pair'
    exchange_name : str
        Назва біржі (для verbose логів)
    base_col : str | None
        Назва колонки з готовим base (якщо біржа віддає — напр., EdgeX 'Base').
        Якщо None — base парситься з 'Pair'.
    category_hint_col : str | None
        Назва колонки з category-hint від API (напр., Extended 'Category').
    """
    if df.empty:
        return df

    if "Pair" not in df.columns:
        raise ValueError(f"DataFrame для {exchange_name} не має колонки 'Pair'")

    bases, canons, quotes, dexs, names, categories = [], [], [], [], [], []

    for _, row in df.iterrows():
        ticker = str(row["Pair"])
        base_hint = str(row[base_col]).strip() if base_col and base_col in df.columns else ""
        cat_hint  = str(row[category_hint_col]).strip() if category_hint_col and category_hint_col in df.columns else ""

        base, canon, quote, hip3 = canonical_base(ticker, base_hint)
        category, asset_name = categorize(canon, cat_hint)

        bases.append(base)
        canons.append(canon)
        quotes.append(quote)
        dexs.append(hip3)
        names.append(asset_name)
        categories.append(category)

    df = df.copy()
    df["Base"]           = bases
    df["Canonical Base"] = canons
    df["Asset Name"]     = names
    df["Category"]       = categories
    df["HIP-3 Dex"]      = dexs
    df["Quote Currency"] = quotes

    return df


# ---------------------------------------------------------------------------
# Допоміжне: manual audit — друк топ-N невідомих для додавання у USER_OVERRIDES
# ---------------------------------------------------------------------------

def print_unknowns(df: pd.DataFrame, exchange_name: str = "", top_n: int = 30) -> None:
    """
    Друкує топ-N canonical bases без Asset Name (потребують ручної класифікації).
    """
    if df.empty or "Asset Name" not in df.columns:
        return

    unknown = df[df["Asset Name"].fillna("").str.len() == 0].copy()
    if unknown.empty:
        return

    vol_col = "Volume 24h (USD)" if "Volume 24h (USD)" in df.columns else None
    if vol_col:
        grouped = unknown.groupby("Canonical Base")[vol_col].sum().sort_values(ascending=False)
    else:
        grouped = unknown.groupby("Canonical Base").size().sort_values(ascending=False)

    if grouped.empty:
        return

    prefix = f"[{exchange_name}] " if exchange_name else ""
    print(f"   🔍 {prefix}Без Asset Name: {len(unknown)} pairs (top {min(top_n, len(grouped))}):")
    for canon, vol in grouped.head(top_n).items():
        if vol_col:
            print(f"      • {canon:20s} ${vol:>15,.0f}")
        else:
            print(f"      • {canon:20s} (count: {vol})")


# ---------------------------------------------------------------------------
# Verbose summary (викликається з fetch.py після всіх бірж)
# ---------------------------------------------------------------------------

def print_summary(all_dfs: dict[str, pd.DataFrame]) -> None:
    """
    Друкує підсумок категоризації через усі біржі.
    Викликається в кінці fetch.py з dict {exchange_name: df_with_categories}.
    """
    combined = []
    for name, df in all_dfs.items():
        if df.empty:
            continue
        tmp = df[["Category", "Canonical Base", "Asset Name"]].copy()
        tmp["Exchange"] = name
        if "Volume 24h (USD)" in df.columns:
            tmp["Volume 24h (USD)"] = df["Volume 24h (USD)"]
        else:
            tmp["Volume 24h (USD)"] = 0
        combined.append(tmp)

    if not combined:
        print("❌ Жодного DataFrame для summary")
        return

    all_df = pd.concat(combined, ignore_index=True)

    print("\n" + "═" * 60)
    print(f"🎨 Категоризація {len(all_df):,} пар:")
    print("─" * 60)

    by_cat = all_df.groupby("Category").agg(
        pairs=("Canonical Base", "count"),
        unique_bases=("Canonical Base", "nunique"),
        volume=("Volume 24h (USD)", "sum"),
    ).reindex(CATEGORIES + (["Unknown"] if "Unknown" in all_df["Category"].unique() else []))

    for cat, row in by_cat.iterrows():
        if pd.isna(row["pairs"]):
            continue
        emoji = CATEGORY_EMOJI.get(cat, "⚠️")
        print(f"   {emoji} {cat:12s}: {int(row['pairs']):>5} pairs | "
              f"{int(row['unique_bases']):>4} unique bases | ${row['volume']:>15,.0f}")

    total_vol = all_df["Volume 24h (USD)"].sum()
    no_name = (all_df["Asset Name"].fillna("").str.len() == 0).sum()
    print("─" * 60)
    print(f"   📊 TOTAL: {len(all_df):,} pairs | ${total_vol:,.0f}")
    if no_name > 0:
        print(f"   🔍 Без Asset Name: {no_name} pairs (додайте у USER_OVERRIDES)")
    print("═" * 60 + "\n")
