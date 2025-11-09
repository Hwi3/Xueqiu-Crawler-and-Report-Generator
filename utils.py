import re
import orjson
import logging
from pathlib import Path
from typing import Any, Dict, List
from config import TICKER_PATTERNS

logger = logging.getLogger("utils")

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def read_json_list(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    raw = path.read_bytes()
    data = orjson.loads(raw)
    out: List[Dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, str):
                # try parse stringified JSON
                try:
                    obj = orjson.loads(item)
                    out.append(obj if isinstance(obj, dict) else {"text": item})
                except:
                    logger.error(f"String item not JSON, trunc: {item[:80]}")
                    out.append({"text": item})
    else:
        logger.error(f"Corrupt JSON root: {type(data)}; returning []")
    return out

def append_unique_json(path: Path, new_items: List[Dict[str, Any]], unique_keys=("id","tab","text_hash")):
    """Append and dedupe by keys. Creates file if missing."""
    existing = read_json_list(path)
    # map key tuple to index
    def key(d: Dict[str, Any]):
        return tuple(d.get(k) for k in unique_keys)
    seen = {key(d) for d in existing}
    added = 0
    for it in new_items:
        k = key(it)
        if k not in seen:
            existing.append(it)
            seen.add(k)
            added += 1
    path.write_bytes(orjson.dumps(existing, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS | orjson.OPT_SORT_KEYS))
    return added, len(existing)

def save_json_list(path: Path, data: List[Dict[str, Any]]):
    path.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS | orjson.OPT_SORT_KEYS))

def detect_symbols(text: str) -> List[str]:
    syms = set()
    for pat in TICKER_PATTERNS:
        for m in re.findall(pat, text or ""):
            syms.add(m.upper())
    return sorted(syms)
