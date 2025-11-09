import time
import json
import logging
from typing import Dict, Any, List
from pathlib import Path
from tqdm import tqdm
import requests
import os
from dotenv import load_dotenv

logger = logging.getLogger("summarizer")

FIREWORKS_URL = "https://api.fireworks.ai/inference/v1/chat/completions"
MODEL = "accounts/fireworks/models/gpt-oss-20b"


# -------------------------------------------------------
# Build prompt
# -------------------------------------------------------
def build_prompt(post: Dict[str, Any]) -> str:
    text = post.get("text", "").strip()
    html = post.get("html", "").strip()

    return f"""
            Please analyze the following Xueqiu investor post and produce a STRICT JSON output (UTF-8, no extra text):

            Required fields:
            - "summary": 1–2 sentence English summary, factual only.
            - "sentiment": one of ["positive", "neutral", "negative"].
            - "themes": 2–5 short English topic labels.
            - "entities": 0–5 companies/people explicitly mentioned.

            Rules:
            - No fabrication.
            - Only use details from the text.
            - Output pure JSON only.

            Post text:
            {text}

            HTML (optional):
            {html}
            """


# -------------------------------------------------------
# HTTP POST request to Fireworks
# -------------------------------------------------------

def fireworks_chat(prompt: str, api_key: str) -> str:
    payload = {
        "model": MODEL,
        "max_tokens": 1024,
        "temperature": 0.3,
        "top_p": 1,
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    response = requests.post(FIREWORKS_URL, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        raise RuntimeError(
            f"Fireworks API error {response.status_code}: {response.text}"
        )

    data = response.json()
    return data["choices"][0]["message"]["content"]


# -------------------------------------------------------
# Summarize one post with retry
# -------------------------------------------------------
def summarize_one(post: Dict[str, Any],
                  api_key: str,
                  retries: int = 5) -> Dict[str, Any] | None:

    if isinstance(post, str):
        logger.error(f"Post is string, not JSON: {post[:20]}")
        return None

    prompt = build_prompt(post)

    for attempt in range(1, retries + 1):
        try:
            raw_output = fireworks_chat(prompt, api_key)

            try:
                parsed = json.loads(raw_output)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON for post {post.get('id')}:\n{raw_output}")
                return None

            parsed["id"] = post.get("id")
            parsed["tab"] = post.get("tab")
            return parsed

        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for post {post.get('id')}: {e}")

            # rate limit
            if "rate limit" in str(e).lower() or "429" in str(e):
                sleep_time = attempt * 1.2
            else:
                sleep_time = 1.0

            time.sleep(sleep_time)

    logger.error(f"FAILED after {retries} retries → post {post.get('id')}")
    return None


# -------------------------------------------------------
# Summarize entire tab
# -------------------------------------------------------
def summarize_tab(job_dir: Path, tab: str, limit: int | None):
    
    if os.path.exists(".env"):
        load_dotenv(".env")
    else:
        raise RuntimeError("Missing FIREWORKS_API_KEY env variable")
    api_key = os.getenv("FIREWORKS_API_KEY")
    if not api_key:
        raise RuntimeError("Missing FIREWORKS_API_KEY env variable")

    raw_file = job_dir / "raw" / f"posts_{tab}.json"
    if not raw_file.exists():
        logger.warning(f"No raw posts for tab {tab}")
        return

    with open(raw_file, "r", encoding="utf-8") as f:
        posts: List[Dict[str, Any]] = json.load(f)

    if limit is not None:
        posts = posts[:limit]

    print(f"\n Summarizing {len(posts)} posts from tab '{tab}'...\n")

    output_dir = job_dir / "summary"
    output_dir.mkdir(exist_ok=True)

    summary_path = output_dir / f"summary_{tab}.json"

    results = []
    for post in tqdm(posts, desc=f"Summarizing [{tab}]", ncols=100):
        res = summarize_one(post, api_key)
        if res:
            results.append(res)
        time.sleep(0.5)   # avoid 429 rate limit

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Saved summaries → {summary_path}")
