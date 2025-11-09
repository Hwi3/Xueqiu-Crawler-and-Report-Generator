from pathlib import Path
from datetime import datetime, timezone

# Storage
PROJECT_ROOT = Path(__file__).parent
STORAGE_ROOT = PROJECT_ROOT / "storage"
STORAGE_ROOT.mkdir(exist_ok=True)

def ts():
    return datetime.now(timezone.utc).isoformat()

def default_jobname():
    # e.g., run_20251108_003825
    return "run_" + datetime.now().strftime("%Y%m%d_%H%M%S")

# Tabs and CSS selectors (fall back aware)
TABS = {
    # 显示名: (key, top_nav_text)
    "热门": ("hot", "热门"),
    "7x24": ("7x24", "7x24"),
    "视频": ("video", "视频"),
    "基金": ("fund", "基金"),
    "资讯": ("news", "资讯"),
    "达人": ("expert", "达人"),
    "私募": ("private_equity", "私募"),
    "ETF": ("etf", "ETF"),
}

# Known per-tab post container selectors (with fallbacks)
TAB_SELECTORS = {
    "hot": [
        "article.style_timeline__item_3WW",           # main feed cards
        "div.style_timeline__item_3WW",
    ],
    "7x24": [
        "table.AnonymousHome_home__timeline-live__tb_2kb tr",
    ],
    "video": [
        "article.style_timeline__item_3WW",
        "div.style_timeline__item_3WW",
    ],
    "fund": [
        "article.style_timeline__item_3WW",
        "div.style_timeline__item_3WW",
    ],
    "news": [
        "article.style_timeline__item_3WW",
        "div.style_timeline__item_3WW",
        "table.AnonymousHome_home__timeline-live__tb_2kb tr",
    ],
    "expert": [
        "article.style_timeline__item_3WW",
    ],
    "private_equity": [
        "article.style_timeline__item_3WW",
    ],
    "etf": [
        "article.style_timeline__item_3WW",
        "table.AnonymousHome_home__timeline-live__tb_2kb tr",
    ],
}

# How many scroll rounds per tab
DEFAULT_SCROLL_ROUNDS = 6

# Regex patterns for ticker detection
TICKER_PATTERNS = [
    r"\bS[HZ]\d{6}\b",      # SH600519, SZ000001
    r"\bHK\d{4,5}\b",      # HK09888
    r"\b\d{6}\.[A-Z]{2}\b" # 600519.SH style
]
