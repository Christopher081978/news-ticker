"""
News-Pipeline (kostenlos, ohne API-Key)
Zeitfenster: Heute + Gestern (Mo=Fr–Mo; Sa=Fr–Sa; So=Sa–So)

Modi:
- WINDOW: Immer ALLE Meldungen im Zeitfenster (Testmodus)
- DELTA : Nur NEUE Meldungen seit letztem Lauf (Produktivmodus) [Standard]

Quellen:
- Google News RSS (mehrere Suchqueries je Asset)
- Firmen-RSS/Atom (IR/Newsroom) mit Autodiscovery & Validierung (asset-spezifisch)
- Globale News-Feeds (Handelsblatt, FAZ, …) -> Treffer werden Assets zugeordnet

Persistenz: SQLite (seen-Hashes + last_run_utc) – nur im DELTA-Modus
Ausgabe: gruppierte Markdown-Liste; optional CSV/JSON, inkl. Quellenlabel (gnews / rss:<domain>)

+ Webhook-Ausgabe:
  - Fester JSON-POST an webhook.site (Variable WEBHOOK_URL).
  - Zusätzlich lokale Ausgabe (Markdown) und CSV/JSON-Datei.
"""

import os, sys, sqlite3, hashlib, requests, xml.etree.ElementTree as ET, csv, re, html as ihtml, json
from datetime import datetime, date, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus, urlparse, urljoin

# ---------- Optional: yfinance für Kursänderungen ----------
try:
    import yfinance as yf  # pip install yfinance
    _YF_OK = True
except Exception:
    _YF_OK = False

# ----------------- Konfiguration -----------------
def _choose_fetch_mode() -> str:
    env = (os.getenv("FETCH_MODE") or "").strip().upper()
    if env in {"WINDOW", "DELTA"}:
        return env
    if "ipykernel" in sys.modules:
        return "DELTA"
    try:
        if sys.stdin.isatty():
            val = input("Fetch-Modus wählen (WINDOW/DELTA) [Standard=DELTA]: ").strip().upper()
            if val in {"WINDOW", "DELTA"}:
                return val
    except Exception:
        pass
    return "DELTA"

FETCH_MODE = _choose_fetch_mode()
print(f"[INFO] FETCH_MODE={FETCH_MODE}")
if not _YF_OK:
    print("[WARN] yfinance nicht installiert – Kursänderungen werden als 'n/a' angezeigt.")

DB_PATH = "news_state.db"
CSV_PATH = "news_delta.csv"
JSON_PATH = "news_delta.json"
WRITE_CSV = True
WRITE_JSON = True

# ---- Webhook (fest verdrahtet) ----
WEBHOOK_URL   = "https://webhook.site/3436a458-4f7a-4fc3-ae83-fd8f577200ac"
WEBHOOK_SECRET = "news-secret-123"   # optionaler Prüf-Header
SEND_WEBHOOK  = True                 # erzwingt POST

TZ_BERLIN = ZoneInfo("Europe/Berlin")
TZ_UTC = timezone.utc
MAX_PER_ASSET = 30
REQUEST_TIMEOUT = 20
UA = {"User-Agent": "Mozilla/5.0 (compatible; NewsDeltaBot/2.0)"}

# ANSI-Optik für TTY
USE_ANSI = sys.stdout.isatty()
ANSI_BOLD = "\033[1m" if USE_ANSI else ""
ANSI_CYAN = "\033[96m" if USE_ANSI else ""
ANSI_RESET = "\033[0m" if USE_ANSI else ""

# ---- Domain-Filter ----
DOMAIN_WHITELIST = {"handelsblatt.com", "handelsblatt.de", "faz.net"}
DOMAIN_BLACKLIST = {
    "bloomberg.com", "ft.com", "financialtimes.com", "wsj.com",
    "nytimes.com", "economist.com", "barrons.com", "marketwatch.com",
    "seekingalpha.com", "reuters.com", "welt.de"
}

# ----------------- Assets -----------------
ASSETS = {
    "UnitedHealth": ["UNH", "UnitedHealth", "UnitedHealth Group"],
    "Rheinmetall": ["RHM.DE", "Rheinmetall"],
    "JD.com": ["JD.com", "JD"],
    "Panasonic": ["6752.T", "Panasonic Holdings", "Panasonic"],
    "XRP": ["XRP-USD", "XRP", "Ripple XRP"],
    "BP": ["BP", "BP PLC", "BP p.l.c."],
    "Novo Nordisk": ["NVO", "Novo Nordisk"],
    "CrowdStrike": ["CRWD", "CrowdStrike"],
    "Unilever": ["ULVR.L", "UL", "Unilever"],
    "Alphabet": ["GOOGL", "GOOG", "Alphabet Google", "Alphabet Inc", "Google"],
    "Sprott Physical Silver Trust": ["PSLV", "Sprott Physical Silver Trust"],
    "VanEck MOAT UCITS": ["MOAT", "VanEck Moat UCITS", "Wide Moat ETF"],
    "BYD": ["1211.HK", "BYD"],
    "Pfizer": ["PFE", "Pfizer"],
    "Air Liquide": ["AI.PA", "Air Liquide"],
    "TSMC": ["TSM", "Taiwan Semiconductor"],
    "NVIDIA": ["NVDA", "NVIDIA"],
    "Siemens": ["SIE.DE", "Siemens"],
    "BASF": ["BAS.DE", "BASF"],
    "Coinbase": ["COIN", "Coinbase"],
    "Ethereum": ["ETH-USD", "ETH Ethereum"],
    "Bitcoin": ["BTC-USD", "BTC Bitcoin"],
    "Solana": ["SOL-USD", "Solana"],
    "D-Wave Quantum": ["QBTS", "D-Wave Quantum"],
    "Robinhood": ["HOOD", "Robinhood Markets", "Robinhood"],
    "Rocket Lab": ["RKLB", "Rocket Lab"],
}

PRIMARY_TICKER_OVERRIDES = {"Alphabet": "GOOGL"}

# ----------------- Firmenfeeds (asset-spezifisch) -----------------
EXTRA_RSS_FEED_URLS = {
    "CrowdStrike": [
        "https://ir.crowdstrike.com/rss/news-releases.xml",
        "https://ir.crowdstrike.com/rss/sec-filings.xml",
        "https://ir.crowdstrike.com/rss/events.xml",
    ],
    "NVIDIA": ["https://nvidianews.nvidia.com/rss"],
    "Alphabet": ["https://blog.google/feed/"],
    "Panasonic": ["https://news.panasonic.com/global/rss/press/index.xml"],
}
EXTRA_RSS_SITE_URLS = {
    "JD.com": ["https://ir.jd.com/news-releases", "https://ir.jd.com/"],
    "Pfizer": ["https://www.pfizer.com/news/press-releases", "https://www.pfizer.com/newsroom"],
    "Siemens": ["https://press.siemens.com/global/en"],
    "BASF": ["https://www.basf.com/us/en/media/news-releases"],
    "Unilever": ["https://www.unilever.com/news/press-and-media/press-releases/"],
    "Coinbase": ["https://www.coinbase.com/blog"],
    "Air Liquide": ["https://www.airliquide.com/group/press-releases-news"],
    "TSMC": ["https://pr.tsmc.com/english/latest-news"],
    "BP": ["https://www.bp.com/en/global/corporate/news-and-insights/press-releases.html"],
    "UnitedHealth": ["https://www.unitedhealthgroup.com/newsroom.html"],
    "Novo Nordisk": ["https://www.novonordisk.com/news-and-media/news-and-ir-materials.html"],
    "Robinhood": ["https://investors.robinhood.com/press-releases", "https://newsroom.aboutrobinhood.com/"],
    "Rocket Lab": ["https://rocketlabcorp.com/updates/", "https://www.rocketlabusa.com/news/"],
}

# ----------------- Globale Newsfeeds -----------------
GLOBAL_RSS_FEEDS = [
    "https://www.handelsblatt.com/contentexport/feed/schlagzeilen",
    "https://www.handelsblatt.com/contentexport/feed/wirtschaft",
    "https://www.handelsblatt.com/contentexport/feed/top-themen",
    "https://www.handelsblatt.com/contentexport/feed/finanzen",
    "https://www.handelsblatt.com/contentexport/feed/marktberichte",
    "https://www.handelsblatt.com/contentexport/feed/unternehmen",
    "https://www.handelsblatt.com/contentexport/feed/politik",
    "https://www.handelsblatt.com/contentexport/feed/technik",
    "https://www.handelsblatt.com/contentexport/feed/arts-style",
    "https://www.handelsblatt.com/contentexport/feed/research-institut",
    "https://www.faz.net/rss/aktuell/",
    "https://www.faz.net/rss/wirtschaft/",
]

# ----------------- Regeln (Alphabet/JD.com) -----------------
ASSET_RULES = {
    "Alphabet": {
        "strong_patterns": [r"\balphabet\b", r"\bgoogle\b"],
        "ticker_terms": [r"\bgoogl\b", r"\bgoog\b"],
        "ticker_requires": ["nasdaq", "nyse", "sec", "earnings", "revenue", "guidance", "quarter"],
        "context_terms": ["search", "youtube", "android", "waymo", "gemini", "cloud", "play store"],
        "soft_excludes": ["amazon", "hertz", "prime", "aws", "whole foods", "kindle", "ring"],
        "disambiguate": None,
        "min_score": 1.5,
    },
    "JD.com": {
        "strong_patterns": [r"\bjd\.com\b", r"\b京东\b"],
        "ticker_terms": [r"\b(?:nasdaq:)?jd\b", r"\b\((?:nasdaq:)?jd\)\b"],
        "ticker_requires": ["nasdaq", "nyse", "adr", "earnings", "revenue", "guidance", "quarter", "ticker", "listed"],
        "context_terms": ["e-commerce", "ecommerce", "china", "chinese", "retail", "logistics", "fulfillment", "marketplace"],
        "soft_excludes": ["jd vance", "j.d. vance", "senator vance", "vance", "j.d. power", "jd power"],
        "disambiguate": "jd_com_disambiguate",
        "min_score": 1.5,
    },
}

# ----------------- Hilfs-/Parsingfunktionen -----------------
def sha(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()
def window_start_berlin(today: date) -> datetime:
    wd = today.weekday()
    start_day = today - timedelta(days=3) if wd == 0 else today - timedelta(days=1)
    return datetime.combine(start_day, time(0, 0), TZ_BERLIN)
def domain_from_url(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""
def domain_allowed(url: str) -> bool:
    d = domain_from_url(url)
    if not d: return False
    if d in DOMAIN_WHITELIST: return True
    if d in DOMAIN_BLACKLIST: return False
    return True
def build_query(asset: str, raw_query: str) -> str:
    base_tail = "(stock OR share OR earnings OR revenue OR guidance OR investment OR funding OR bond OR ETF OR crypto)"
    return f"({raw_query}) {base_tail}"
def gnews_rss_url(asset: str, query: str) -> str:
    q = build_query(asset, query)
    return f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[\W_]+")
def _clean_text(s: str) -> str:
    s = ihtml.unescape(_TAG_RE.sub(" ", s))
    return _WS_RE.sub(" ", s).strip()
def _norm(s: str) -> str: return _PUNCT_RE.sub("", s or "").lower().strip()
def _first_sentences(text: str, max_sentences: int = 3, max_len: int = 360) -> str:
    parts = re.split(r"(?<=[\.\!\?])\s+", text.strip()); out, total = [], 0
    for p in parts:
        p = p.strip()
        if not p: continue
        out.append(p); total += len(p)
        if len(out) >= max_sentences or total >= max_len: break
    return " ".join(out) if out else text[:max_len].rstrip()
def summarize_from_rss_elem(item_elem: ET.Element, title: str) -> str | None:
    desc = (item_elem.findtext("description")
            or item_elem.findtext("{http://www.w3.org/2005/Atom}summary")
            or item_elem.findtext("{http://www.w3.org/2005/Atom}content"))
    if not desc: return None
    desc = _clean_text(desc)
    if not desc: return None
    if any(tok in desc for tok in ["{", "}", ";", "font-family", "cookie"]): return None
    if _norm(desc).startswith(_norm(title)) or _norm(desc) == _norm(title): return None
    return _first_sentences(desc)
def parse_pubdate(elem: ET.Element) -> datetime | None:
    pd = (elem.findtext("pubDate")
          or elem.findtext("{http://www.w3.org/2005/Atom}updated")
          or elem.findtext("{http://www.w3.org/2005/Atom}published"))
    if not pd: return None
    try:
        return parsedate_to_datetime(pd).astimezone(TZ_UTC)
    except Exception:
        try:
            dt = datetime.fromisoformat(pd.replace("Z", "+00:00"))
            return dt.astimezone(TZ_UTC)
        except Exception:
            return None
def is_feed_document(xml_root: ET.Element) -> bool:
    tag = xml_root.tag.lower()
    return tag.endswith("rss") or tag.endswith("feed")
def fetch_xml(url: str) -> ET.Element | None:
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers=UA)
        r.raise_for_status()
        return ET.fromstring(r.content)
    except Exception:
        return None
def discover_feed_links(page_url: str) -> list[str]:
    try:
        r = requests.get(page_url, timeout=REQUEST_TIMEOUT, headers=UA); r.raise_for_status(); html = r.text
    except Exception:
        return []
    feeds = []
    for tag in re.findall(r'<link[^>]+rel=["\']alternate["\'][^>]+>', html, flags=re.I):
        if re.search(r'type=["\']application/(rss\+xml|atom\+xml)["\']', tag, flags=re.I):
            m = re.search(r'href=["\']([^"\']+)["\']', tag, flags=re.I)
            if m: feeds.append(m.group(1))
    for m in re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>', html, flags=re.I):
        if re.search(r'(rss|atom|feed|\.xml)$', m, flags=re.I):
            feeds.append(m)
    out, seen = [], set()
    for href in feeds:
        if href.startswith("//"): href = ("https:" if page_url.startswith("https") else "http:") + href
        elif href.startswith("/"): p = urlparse(page_url); href = f"{p.scheme}://{p.netloc}{href}"
        elif not href.startswith("http"): href = urljoin(page_url, href)
        if href not in seen: seen.add(href); out.append(href)
    return out
def fetch_feed_url(feed_url: str, source_hint: str = "") -> list[tuple[str, str, datetime, str | None, str]]:
    root = fetch_xml(feed_url)
    if root is None or not is_feed_document(root): return []
    out = []
    ch = root.find("channel")
    if ch is not None:
        for it in ch.findall("item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            dt_utc = parse_pubdate(it)
            if not (title and link and dt_utc): continue
            rss_sum = summarize_from_rss_elem(it, title)
            out.append((title, link, dt_utc, rss_sum, f"rss:{domain_from_url(feed_url) or source_hint}"))
        return out
    for it in root.findall("{http://www.w3.org/2005/Atom}entry"):
        title = (it.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
        link_el = it.find("{http://www.w3.org/2005/Atom}link")
        link = (link_el.get("href") if link_el is not None else "").strip()
        dt_utc = parse_pubdate(it)
        if not (title and link and dt_utc): continue
        rss_sum = summarize_from_rss_elem(it, title)
        out.append((title, link, dt_utc, rss_sum, f"rss:{domain_from_url(feed_url) or source_hint}"))
    return out

# ----------------- Disambiguation & Matching -----------------
def jd_com_disambiguate(text_lower: str) -> bool:
    if ("j.d. power" in text_lower or "jd power" in text_lower) and ("jd.com" not in text_lower):
        return True
    if "vance" in text_lower and ("jd.com" not in text_lower):
        commerce_markers = ("e-commerce", "ecommerce", "retail", "marketplace", "china", "chinese", "logistics")
        if not any(k in text_lower for k in commerce_markers): return True
    return False
_DISAMBIGUATE_FUNCS = {"jd_com_disambiguate": jd_com_disambiguate}

FINANCE_TERMS = [
    "nasdaq","nyse","lse","six","tsx","hkex","xetra",
    "stock","share","shares","trading","ipo","adr",
    "earnings","revenue","guidance","profit","loss",
    "dividend","buyback","quarter","results","sec","filing"
]
def _count_hits(text: str, terms: list[str]) -> int:
    return sum(1 for w in terms if w in text) if terms else 0
def _regex_hits(text: str, patterns: list[str]) -> int:
    hits = 0
    for pat in (patterns or []):
        hits += len(re.findall(pat, text, flags=re.I))
    return hits
def _generic_match(asset: str, title: str, rss_text: str | None, query_hint: str | None) -> bool:
    text = f"{title}\n{rss_text or ''}".lower()
    aliases = [asset] + ASSETS.get(asset, [])
    for a in aliases:
        al = (a or "").strip()
        if not al: continue
        al_low = al.lower()
        if len(al_low) >= 3 and not al.isupper() and al_low in text:
            return True
    tickers = []
    for a in aliases:
        if not a: continue
        core = a.split(".")[0]
        if 2 <= len(core) <= 5 and core.isalnum() and core.upper() == core:
            tickers.append(core.lower())
    if tickers:
        fin = _count_hits(text, FINANCE_TERMS) > 0
        if fin and any(re.search(rf"\b{re.escape(t)}\b", text) for t in tickers):
            return True
    if query_hint and len(query_hint) >= 3 and (query_hint.lower() in text):
        return True
    return False
def evaluate_asset_match_gnews(asset: str, title: str, rss_text: str | None, query_hint: str | None) -> bool:
    rules = ASSET_RULES.get(asset)
    t = (title or ""); s = (rss_text or "")
    text_lower = f"{t}\n{s}".lower()
    if rules:
        dis_fn_name = rules.get("disambiguate")
        if dis_fn_name:
            fn = _DISAMBIGUATE_FUNCS.get(dis_fn_name)
            if fn and fn(text_lower): return False
        score = 0.0
        score += 2.0 * _regex_hits(text_lower, rules.get("strong_patterns", []))
        ticker_ok = _regex_hits(text_lower, rules.get("ticker_terms", [])) > 0
        requires_ok = _count_hits(text_lower, [w.lower() for w in rules.get("ticker_requires", [])]) > 0
        if ticker_ok and requires_ok: score += 1.0
        ctx_hits = _count_hits(text_lower, [w.lower() for w in rules.get("context_terms", [])]); score += min(1.0, 0.5 * ctx_hits)
        soft_ex = [w.lower() for w in rules.get("soft_excludes", [])]; soft_hit = any(w in text_lower for w in soft_ex)
        min_score = float(rules.get("min_score", 1.0))
        return (score >= min_score) if soft_hit else (score > 0.0)
    return _generic_match(asset, t, s, query_hint)
def evaluate_asset_match_article(asset: str, title: str, rss_text: str | None) -> bool:
    rules = ASSET_RULES.get(asset)
    t = (title or ""); s = (rss_text or "")
    text_lower = f"{t}\n{s}".lower()
    if asset == "JD.com":
        if "jd.com" in text_lower or "京东" in text_lower:
            pass
        elif re.search(r"\bjd\b", text_lower) and _count_hits(text_lower, FINANCE_TERMS) > 0:
            pass
        else:
            return False
    if rules:
        dis_fn_name = rules.get("disambiguate")
        if dis_fn_name:
            fn = _DISAMBIGUATE_FUNCS.get(dis_fn_name)
            if fn and fn(text_lower): return False
        if _regex_hits(text_lower, rules.get("strong_patterns", [])) > 0:
            return True
        if _regex_hits(text_lower, rules.get("ticker_terms", [])) > 0 and _count_hits(text_lower, FINANCE_TERMS) > 0:
            return True
        if _count_hits(text_lower, [w.lower() for w in rules.get("context_terms", [])]) > 0:
            if not any(x in text_lower for x in [w.lower() for w in rules.get("soft_excludes", [])]):
                return True
    return _generic_match(asset, t, s, query_hint=None)

# ----------------- Persistenz -----------------
def _column_names(cur, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def init_db():
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS seen(
        hash TEXT PRIMARY KEY,
        url TEXT, title TEXT, published_at_utc TEXT, asset TEXT, source TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS meta(
        key TEXT PRIMARY KEY, value TEXT
    )""")
    # Migration (robust)
    cols = _column_names(cur, "seen")
    if "source" not in cols:
        try: cur.execute("ALTER TABLE seen ADD COLUMN source TEXT")
        except Exception: pass
    if "published_at_utc" not in cols:
        try: cur.execute("ALTER TABLE seen ADD COLUMN published_at_utc TEXT")
        except Exception: pass
    con.commit(); con.close()

def get_last_run_utc() -> datetime | None:
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("SELECT value FROM meta WHERE key='last_run_utc'"); row = cur.fetchone()
    con.close()
    if not row: return None
    return datetime.fromisoformat(row[0]).replace(tzinfo=TZ_UTC)
def set_last_run_utc(dt_utc: datetime):
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("INSERT INTO meta(key,value) VALUES('last_run_utc',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (dt_utc.replace(tzinfo=TZ_UTC).isoformat(),))
    con.commit(); con.close()
def is_seen(h: str) -> bool:
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.execute("SELECT 1 FROM seen WHERE hash=? LIMIT 1", (h,))
    ok = cur.fetchone() is not None
    con.close(); return ok
def mark_seen(items: list[dict]):
    if not items: return
    con = sqlite3.connect(DB_PATH); cur = con.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO seen(hash,url,title,published_at_utc,asset,source) VALUES(?,?,?,?,?,?)",
        [(it["hash"], it["url"], it["title"], it["published_at_utc"], it["asset"], it.get("source","")) for it in items]
    )
    con.commit(); con.close()

# ----------------- Kursänderungen (Yahoo Finance) -----------------
def _pick_primary_ticker(asset: str) -> str | None:
    if asset in PRIMARY_TICKER_OVERRIDES:
        return PRIMARY_TICKER_OVERRIDES[asset]
    aliases = ASSETS.get(asset, [])
    for a in aliases:
        if not a or " " in a: continue
        if re.match(r"^[A-Z0-9\.\-]{2,}$", a):
            return a
    return None
def _prev_business_day(dt_local: datetime) -> datetime:
    wd = dt_local.weekday()
    delta = 1 if wd >= 1 else 3
    return dt_local - timedelta(days=delta)
def _nearest_bar(df, target_ts, max_minutes=30):
    if df is None or df.empty: return None
    diffs = (df.index - target_ts)
    diffs = diffs.map(lambda x: abs(x.total_seconds()))
    idx = diffs.idxmin()
    if abs((idx - target_ts).total_seconds()) <= max_minutes*60:
        return df.loc[idx]
    return None
def _fetch_change_yf(ticker: str, asset: str) -> str:
    if not _YF_OK or not ticker:
        return "n/a"
    try:
        is_de = ticker.endswith(".DE")
        now_berlin = datetime.now(TZ_BERLIN)
        if is_de:
            t_today = now_berlin.replace(hour=9, minute=0, second=0, microsecond=0)
            t_prev = _prev_business_day(t_today)
            start = (t_prev - timedelta(hours=2)).astimezone(timezone.utc)
            end = (t_today + timedelta(hours=2)).astimezone(timezone.utc)
            df = yf.download(ticker, interval="1m", start=start, end=end, prepost=False, progress=False)
            if df is None or df.empty:
                ddf = yf.download(ticker, period="10d", interval="1d", progress=False)
                if ddf is None or len(ddf) < 2: return "n/a"
                p_prev = float(ddf["Open"].iloc[-2]); p_curr = float(ddf["Open"].iloc[-1])
            else:
                t_today_utc = t_today.astimezone(timezone.utc).replace(tzinfo=None)
                t_prev_utc = t_prev.astimezone(timezone.utc).replace(tzinfo=None)
                bar_today = _nearest_bar(df, t_today_utc, 30) or df.iloc[-1]
                bar_prev = _nearest_bar(df, t_prev_utc, 30) or df.iloc[0]
                p_curr = float(bar_today["Close"]); p_prev = float(bar_prev["Close"])
        else:
            df = yf.download(ticker, period="3d", interval="1m", progress=False, prepost=False)
            if df is not None and len(df) >= 2:
                last_ts = df.index[-1]
                target_prev = last_ts - timedelta(days=1)
                bar_prev = _nearest_bar(df.loc[:target_prev + timedelta(minutes=60)], target_prev, 60)
                p_curr = float(df["Close"].iloc[-1])
                if bar_prev is not None:
                    p_prev = float(bar_prev["Close"])
                else:
                    ddf = yf.download(ticker, period="5d", interval="1d", progress=False)
                    if ddf is None or len(ddf) < 2: return "n/a"
                    p_prev = float(ddf["Close"].iloc[-2])
            else:
                ddf = yf.download(ticker, period="5d", interval="1d", progress=False)
                if ddf is None or len(ddf) < 2: return "n/a"
                p_curr = float(ddf["Close"].iloc[-1]); p_prev = float(ddf["Close"].iloc[-2])
        if p_prev <= 0: return "n/a"
        change = (p_curr - p_prev) / p_prev * 100.0
        sign = "+" if change >= 0 else ""
        return f"{sign}{change:.2f}%"
    except Exception:
        return "n/a"

_PRICE_BADGE_CACHE: dict[str, str] = {}
def price_badge_for_asset(asset: str) -> str:
    if asset in _PRICE_BADGE_CACHE:
        return _PRICE_BADGE_CACHE[asset]
    ticker = _pick_primary_ticker(asset)
    badge = "n/a" if not ticker else _fetch_change_yf(ticker, asset)
    label = f"{badge} | {ticker or '-'}"
    _PRICE_BADGE_CACHE[asset] = label
    return label

# ----------------- Ausgabe -----------------
def print_summary_table(per_asset_src: dict):
    headers = ["Asset", "Δ%", "GNews", "GlobalFeeds", "CompanyRSS", "Total"]
    print("\n## Quellen-Übersicht (Treffer je Asset)")
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join(["---"]*len(headers)) + "|")
    for asset in sorted(per_asset_src.keys()):
        gf, gk, crf, crk, grf, grk = per_asset_src[asset]
        total = gk + crk + grk
        badge = price_badge_for_asset(asset)
        delta_only = badge.split("|", 1)[0].strip()
        print(f"| {asset} | {delta_only} | {gk} | {grk} | {crk} | {total} |")

def _fmt_title_bold(title: str) -> str:
    if USE_ANSI:
        return f"{ANSI_BOLD}{ANSI_CYAN}{title}{ANSI_RESET}"
    return f"**{title}**"

def print_markdown(collected: list[dict]):
    buckets: dict[str, list[dict]] = {}
    for it in collected:
        buckets.setdefault(it["asset"], []).append(it)
    for asset in sorted(buckets.keys()):
        badge = price_badge_for_asset(asset)
        print(f"\n### {asset} ({badge})")
        for it in buckets[asset]:
            ts = it["published_at_utc"].replace("T", " ")[:16]
            src = it.get("source","")
            title = _fmt_title_bold(it["title"])
            summary = (it.get("summary") or "").strip()
            src_label = f"  _[{src}]_" if src else ""
            if summary:
                print(f"- {title} ({ts} UTC){src_label}\n  {summary}\n  {it['url']}")
            else:
                print(f"- {title} ({ts} UTC){src_label}\n  {it['url']}")

def write_csv(collected: list[dict], path: str):
    if not collected: return
    cols = ["asset", "query", "published_at_utc", "title", "summary", "url", "source"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for it in collected: w.writerow({k: it.get(k, "") for k in cols})

def write_json(collected: list[dict], path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)

# ----------------- Fetchers -----------------
def fetch_gnews(asset: str, query: str) -> list[tuple[str, str, datetime, str | None, str]]:
    try:
        r = requests.get(gnews_rss_url(asset, query), timeout=REQUEST_TIMEOUT, headers=UA)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception:
        return []
    ch = root.find("channel"); items = [] if ch is None else ch.findall("item")
    out = []
    for it in items:
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        dt_utc = parse_pubdate(it)
        if not (title and link and dt_utc): continue
        rss_sum = summarize_from_rss_elem(it, title)
        out.append((title, link, dt_utc, rss_sum, "gnews"))
    return out
def preload_global_feeds() -> list[tuple[str, str, datetime, str | None, str]]:
    all_items = []
    for u in GLOBAL_RSS_FEEDS:
        got = fetch_feed_url(u)
        if got: all_items.extend(got)
    return all_items
def gather_extra_feeds_for_asset(asset: str) -> list[tuple[str, str, datetime, str | None, str]]:
    results = []
    for u in EXTRA_RSS_FEED_URLS.get(asset, []):
        got = fetch_feed_url(u)
        if got: results.extend(got)
    for page in EXTRA_RSS_SITE_URLS.get(asset, []):
        for f in discover_feed_links(page):
            got = fetch_feed_url(f, source_hint=domain_from_url(page))
            if got: results.extend(got)
    return results

# ----------------- Debug -----------------
def debug_state(prefix: str, per_asset_counts: dict | None = None, per_asset_src: dict | None = None):
    try:
        con = sqlite3.connect(DB_PATH); cur = con.cursor()
        cur.execute("SELECT value FROM meta WHERE key='last_run_utc'"); lr = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM seen"); seen_cnt = cur.fetchone()[0]
        con.close()
    except Exception:
        lr = None; seen_cnt = -1
    print(f"\n[DEBUG] {prefix} | FETCH_MODE={FETCH_MODE}")
    print(f"DB_PATH         : {DB_PATH}")
    print(f"MAX_PER_ASSET   : {MAX_PER_ASSET}")
    if per_asset_counts:
        print("Per-Asset: fetched | <=cutoff/outside | dup | filtered | kept")
        for a, t in sorted(per_asset_counts.items()):
            f, cut, dup, filtered, kept = t
            print(f"  - {a:28s} {f:4d} | {cut:7d} | {dup:3d} | {filtered:8d} | {kept:4d}")

# ----------------- Webhook-POST -----------------
def post_to_webhook(collected: list[dict], per_asset_src: dict, started_utc: datetime, finished_utc: datetime) -> int:
    if not SEND_WEBHOOK:
        return 0
    try:
        payload = {
            "generated_at": finished_utc.replace(tzinfo=TZ_UTC).isoformat().replace("+00:00","Z"),
            "fetch_mode": FETCH_MODE,
            "window_start_utc": window_start_utc_now().isoformat().replace("+00:00","Z"),
            "count": len(collected),
            "per_asset_sources": {
                a: {"gnews_f": v[0], "gnews_k": v[1], "company_f": v[2], "company_k": v[3], "global_f": v[4], "global_k": v[5]}
                for a, v in per_asset_src.items()
            },
            "items": [
                {
                    "asset": it["asset"],
                    "query": it.get("query",""),
                    "title": it["title"],
                    "summary": it.get("summary",""),
                    "url": it["url"],
                    "source": it.get("source",""),
                    "published_at_utc": it["published_at_utc"]
                } for it in collected
            ]
        }
        headers = {"X-Webhook-Secret": WEBHOOK_SECRET}
        r = requests.post(WEBHOOK_URL, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        print(f"[INFO] Webhook POST -> {r.status_code}")
        return r.status_code
    except Exception as e:
        print(f"[WARN] Webhook POST fehlgeschlagen: {e}")
        return -1

# ----------------- Hauptlauf -----------------
def window_start_utc_now():
    start_local = window_start_berlin(datetime.now(TZ_BERLIN).date())
    return start_local.astimezone(TZ_UTC)

def run_once():
    init_db()
    print("[INFO] Globale Newsfeeds (Handelsblatt/FAZ) + Firmen-RSS + Google News werden abgefragt …")
    started_utc = datetime.now(TZ_UTC)
    now_utc = started_utc
    window_start_utc = window_start_utc_now()

    cutoff_utc = window_start_utc
    if FETCH_MODE == "DELTA":
        last_run = get_last_run_utc()
        if last_run and last_run > cutoff_utc:
            cutoff_utc = last_run

    global_items = preload_global_feeds()

    collected = []
    per_asset = {}
    per_asset_src = {}

    for asset, queries in ASSETS.items():
        bucket = []
        f_total = cut_cnt = dup_cnt = filt_cnt = 0
        gnews_f = gnews_k = comp_rss_f = comp_rss_k = glob_rss_f = glob_rss_k = 0

        # 1) Google News
        for q in queries:
            for title, link, dt_utc, rss_sum, src in fetch_gnews(asset, q):
                gnews_f += 1; f_total += 1
                if dt_utc < window_start_utc or dt_utc > now_utc: cut_cnt += 1; continue
                if FETCH_MODE == "DELTA" and dt_utc <= cutoff_utc: cut_cnt += 1; continue
                if not domain_allowed(link): filt_cnt += 1; continue
                if not evaluate_asset_match_gnews(asset, title, rss_sum, query_hint=q): filt_cnt += 1; continue
                h = sha((title + link).lower())
                if FETCH_MODE == "DELTA" and is_seen(h): dup_cnt += 1; continue
                bucket.append({
                    "asset": asset, "query": q, "title": title, "url": link,
                    "summary": (rss_sum or "").strip(),
                    "published_at_utc": dt_utc.isoformat().replace("+00:00", "Z"),
                    "hash": h, "source": src
                })
                gnews_k += 1

        # 2) Firmenfeeds
        for title, link, dt_utc, rss_sum, src in gather_extra_feeds_for_asset(asset):
            comp_rss_f += 1; f_total += 1
            if dt_utc < window_start_utc or dt_utc > now_utc: cut_cnt += 1; continue
            if FETCH_MODE == "DELTA" and dt_utc <= cutoff_utc: cut_cnt += 1; continue
            if not domain_allowed(link): filt_cnt += 1; continue
            h = sha((title + link).lower())
            if FETCH_MODE == "DELTA" and is_seen(h): dup_cnt += 1; continue
            bucket.append({
                "asset": asset, "query": "IR/Press", "title": title, "url": link,
                "summary": (rss_sum or "").strip(),
                "published_at_utc": dt_utc.isoformat().replace("+00:00", "Z"),
                "hash": h, "source": src
            })
            comp_rss_k += 1

        # 3) Globale Feeds
        for title, link, dt_utc, rss_sum, src in global_items:
            glob_rss_f += 1; f_total += 1
            if dt_utc < window_start_utc or dt_utc > now_utc: cut_cnt += 1; continue
            if FETCH_MODE == "DELTA" and dt_utc <= cutoff_utc: cut_cnt += 1; continue
            if not domain_allowed(link): filt_cnt += 1; continue
            if not evaluate_asset_match_article(asset, title, rss_sum): filt_cnt += 1; continue
            h = sha((title + link + asset).lower())
            if FETCH_MODE == "DELTA" and is_seen(h): dup_cnt += 1; continue
            bucket.append({
                "asset": asset, "query": "GlobalFeed", "title": title, "url": link,
                "summary": (rss_sum or "").strip(),
                "published_at_utc": dt_utc.isoformat().replace("+00:00", "Z"),
                "hash": h, "source": src
            })
            glob_rss_k += 1

        # 4) Intra-Asset Dedup & Limit
        seen_local = set(); uniq = []
        def _key(it): return (it["published_at_utc"], 1 if str(it.get("source","")).startswith("rss:") else 0)
        for it in sorted(bucket, key=_key, reverse=True):
            if it["hash"] in seen_local: dup_cnt += 1; continue
            seen_local.add(it["hash"]); uniq.append(it)
            if len(uniq) >= MAX_PER_ASSET: break

        per_asset[asset] = (f_total, cut_cnt, dup_cnt, filt_cnt, len(uniq))
        per_asset_src[asset] = (gnews_f, gnews_k, comp_rss_f, comp_rss_k, glob_rss_f, glob_rss_k)
        collected.extend(uniq)

    # Übersicht
    print_summary_table(per_asset_src)

    collected.sort(key=lambda x: x["published_at_utc"], reverse=True)
    debug_state("Vor Ausgabe", per_asset, per_asset_src)

    if not collected:
        print("\nKeine Artikel im Zeitfenster (heute + gestern).")
    else:
        print_markdown(collected)
        if WRITE_CSV:
            write_csv(collected, CSV_PATH); print(f"\nCSV gespeichert: {CSV_PATH}")
        if WRITE_JSON:
            write_json(collected, JSON_PATH); print(f"JSON gespeichert: {JSON_PATH}")

    # Persistenz + Webhook
    finished_utc = datetime.now(TZ_UTC)
    if FETCH_MODE == "DELTA":
        mark_seen(collected)
        set_last_run_utc(finished_utc)

    if SEND_WEBHOOK:
        post_to_webhook(collected, per_asset_src, started_utc, finished_utc)

# ----------- Hilfsfunktion: Ping-Test -----------
def test_webhook():
    try:
        r = requests.post(
            WEBHOOK_URL,
            json={"ping":"ok","ts":datetime.utcnow().isoformat()+"Z"},
            headers={"X-Webhook-Secret": WEBHOOK_SECRET},
            timeout=REQUEST_TIMEOUT
        )
        print("Webhook Test:", r.status_code)
    except Exception as e:
        print("Webhook Test fehlgeschlagen:", e)

if __name__ == "__main__":
    # 1) Optional: einmal kurz testen
    # test_webhook()
    # 2) Dann regulär laufen lassen
    run_once()
