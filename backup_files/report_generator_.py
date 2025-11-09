import json
from pathlib import Path
from collections import defaultdict, Counter
import datetime


## Helper functions ##
def _safe_ts(raw_post):
    ts_fields = ["timestamp", "created_at", "ts"]
    for k in ts_fields:
        if k in raw_post and raw_post[k]:
            try:
                v = raw_post[k]
                if isinstance(v, (int, float)):
                    iso = datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                    return iso, float(v)
                # Try parse ISO
                try:
                    dt = datetime.datetime.fromisoformat(str(v).replace("Z", ""))
                    return dt.replace(tzinfo=None).isoformat() + "Z", dt.timestamp()
                except Exception:
                    return str(v), None
            except Exception:
                pass
    return None, None


def _parse_ts(raw_post):
    """Return a sortable epoch timestamp from various time fields."""
    ts_iso, ts_epoch = _safe_ts(raw_post)
    if ts_epoch:
        return ts_epoch
    # fallback: if no numeric epoch, try parsing the ISO manually
    try:
        dt = datetime.datetime.fromisoformat(str(ts_iso).replace("Z", ""))
        return dt.timestamp()
    except Exception:
        return 0



## Report Writer###
def generate_report(job_dir: Path, job_name: str) -> str:
    summary_dir = job_dir / "summary"
    raw_dir = job_dir / "raw"
    report_dir = job_dir / "reports"
    report_dir.mkdir(exist_ok=True)

    # ---------- Load data ----------
    all_summaries = []
    for f in summary_dir.glob("*.json"):
        try:
            all_summaries.extend(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            continue

    all_raw = []
    for f in raw_dir.glob("*.json"):
        try:
            all_raw.extend(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            continue

    if not all_summaries:
        raise RuntimeError("No summarized posts found; cannot generate report.")

    raw_by_id = {str(p.get("id")): p for p in all_raw if p.get("id")}

    # ---------- Group by tab ----------
    summaries_by_tab = defaultdict(list)
    for s in all_summaries:
        tab = s.get("tab", "unknown")
        summaries_by_tab[tab].append(s)

    # ---------- Begin report ----------
    lines = []
    now_utc = datetime.datetime.utcnow().isoformat() + "Z"
    lines.append(f"# Xueqiu Sentiment Report (Grouped by Tab) — {job_name}")
    lines.append(f"_Generated {now_utc}_\n")

    lines.append("## Overview")
    lines.append(f"- Total summarized posts: {len(all_summaries)}")
    tabs_list = [str(k) if k else "unknown" for k in summaries_by_tab.keys()]
    lines.append(f"- Tabs found: {', '.join(tabs_list)}")

    lines.append("")

    # ---------- Per-tab sections ----------
    for tab, tab_summaries in summaries_by_tab.items():
        lines.append(f"## 🗂️ Tab: **{tab}**")
        lines.append(f"Total posts: **{len(tab_summaries)}**")

        sentiment_counter = Counter()
        theme_counter = Counter()
        symbol_counter = Counter()
        per_symbol_posts = defaultdict(list)
        per_theme_posts = defaultdict(list)

        for s in tab_summaries:
            sid = str(s.get("id"))
            sentiment = s.get("sentiment", "neutral")
            sentiment_counter[sentiment] += 1

            themes = [t for t in (s.get("themes") or []) if t]
            for t in themes:
                theme_counter[t] += 1
                per_theme_posts[t].append(s)

            raw_post = raw_by_id.get(sid, {})
            symbols = raw_post.get("symbols") or []
            for sym in symbols:
                symbol_counter[sym] += 1
                per_symbol_posts[sym].append(s)

        # Sentiment summary
        lines.append(f"- **Sentiment distribution:** {dict(sentiment_counter)}")

        # Top tickers & themes
        top_syms = [s for s,_ in symbol_counter.most_common(10)]
        top_themes = [t for t,_ in theme_counter.most_common(10)]
        lines.append(f"- **Top tickers:** {', '.join(top_syms) if top_syms else '-'}")
        lines.append(f"- **Top themes:** {', '.join(top_themes) if top_themes else '-'}")
        lines.append("")

        # ---- Special chronological timeline for 7x24 tab ----
        if tab == "7x24":
            lines.append("### 🕒 Chronological Timeline (Latest to Earliest)")
            # Build (timestamp, text, id, url)
            timeline_posts = []
            for s in tab_summaries:
                rid = s.get("id")
                raw = raw_by_id.get(rid, {})
                ts_epoch = _parse_ts(raw)
                ts_iso, _ = _safe_ts(raw)
                txt = (raw.get("text") or "").strip().replace("\n", " ")
                url = raw.get("url")
                timeline_posts.append((ts_epoch, ts_iso, rid, txt, url))

            # sort newest first
            timeline_posts = sorted(timeline_posts, key=lambda x: x[0], reverse=True)

            for ts_epoch, ts_iso, rid, txt, url in timeline_posts:
                head = txt[:260] + ("…" if len(txt) > 260 else "")
                lines.append(f"- **{ts_iso or 'N/A'}** — [{rid}] {head}")
                if url:
                    lines.append(f"  🔗 {url}")
            lines.append("")
            continue  # skip normal theme/ticker detail for this tab

        # Theme details (non-7x24 tabs)
        for theme, _ in theme_counter.most_common(5):
            posts = per_theme_posts[theme][:3]
            lines.append(f"### 🔸 Theme: **{theme}**  |  Mentions: {theme_counter[theme]}")
            for p in posts:
                rid = p.get("id")
                raw_txt = (raw_by_id.get(rid, {}).get("text") or "").strip().replace("\n", " ")
                url = raw_by_id.get(rid, {}).get("url")
                head = raw_txt[:250] + ("…" if len(raw_txt) > 250 else "")
                post_time = raw_by_id.get(rid, {}).get("post_time")
                lines.append(f"> **[{rid}]** {head}")
                if post_time:
                    lines.append(f"> 🕒 {post_time}")
                if url:
                    lines.append(f"> 🔗 {url}")
            lines.append("")

        # Ticker-level details
        lines.append("### 💹 Ticker Mentions (Top 10)")
        lines.append("| Ticker | Mentions | Example IDs |")
        lines.append("|--------|-----------|--------------|")
        for sym, cnt in symbol_counter.most_common(10):
            ids = [p["id"] for p in per_symbol_posts[sym][:3]]
            lines.append(f"| {sym} | {cnt} | {', '.join(ids)} |")
        lines.append("")


    # ---------- Save report ----------
    out_path = report_dir / "report_by_tab.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return str(out_path)
