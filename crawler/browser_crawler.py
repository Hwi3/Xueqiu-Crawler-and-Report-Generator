# crawler/browser_crawler.py

import asyncio
import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from config import TABS, TAB_SELECTORS, DEFAULT_SCROLL_ROUNDS, ts
from utils import ensure_dir, append_unique_json, detect_symbols

logger = logging.getLogger("crawler")
HOME_URL = "https://xueqiu.com/"


def _hash_text(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


def _abs_url(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    return urljoin(HOME_URL, href)


class XueqiuBrowserCrawler:
    def __init__(self, raw_dir: Path, scroll_rounds: int = DEFAULT_SCROLL_ROUNDS):
        self.raw_dir = raw_dir
        ensure_dir(self.raw_dir)
        self.scroll_rounds = scroll_rounds

    # -------------------------------------------------------
    # Safe goto with retry
    # -------------------------------------------------------
    async def safe_goto(self, page, url, wait="domcontentloaded", retries=3):
        for attempt in range(retries):
            try:
                await page.goto(url, wait_until=wait, timeout=60000)
                return True
            except Exception as e:
                logger.warning(f"[Goto Retry {attempt+1}/{retries}] {url} failed: {e}")
                await asyncio.sleep(2)
        logger.error(f"[Goto Fail] Could not load {url} after {retries} attempts.")
        return False

    # -------------------------------------------------------
    # Go to tab safely
    # -------------------------------------------------------
    async def _goto_tab(self, page, tab_label_cn: str):
        success = await self.safe_goto(page, HOME_URL, wait="domcontentloaded")
        if not success:
            return
        try:
            await page.wait_for_selector("div.style_home_timeline_tabs_2Sm a", timeout=10000)
        except:
            logger.warning("Tabs container not found; fallback wait.")
            await page.wait_for_timeout(3000)

        try:
            await page.get_by_role("link", name=tab_label_cn).first.click(timeout=2000)
        except:
            try:
                await page.get_by_text(tab_label_cn, exact=True).first.click(timeout=2000)
            except:
                logger.warning(f"Tab '{tab_label_cn}' not clickable; staying on home.")
        await page.wait_for_timeout(1000)

    async def _load_more(self, page):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1200)

    # -------------------------------------------------------
    # Collect article URLs per tab
    # -------------------------------------------------------
    # async def _collect_links(self, page, tab_key: str) -> List[str]:
    #     html = await page.content()
    #     soup = BeautifulSoup(html, "lxml")
    #     seen = set()

    #     if tab_key in ["hot", "fund", "expert", "private_equity", "etf"]:
    #         ### ADD CODE HERE ###
    #         anchors = soup.select("a.style_fake-anchor_2cg.fake-anchor[href]")
    #         for a in anchors:
    #             href = a.get("href", "")
    #             if href and href.count("/") >= 2 and href.strip("/").split("/")[-1].isdigit():
    #                 absu = urljoin(HOME_URL, href)
    #                 if absu not in seen:
    #                     seen.add(absu)
                
                        
    #     elif tab_key == "news":
    #         anchors = soup.select("a[href*='/S/']")
    #         for a in anchors:
    #             href = a.get("href", "")
    #             if "/S/" in href and href.count("/") >= 3:
    #                 absu = urljoin(HOME_URL, href)
    #                 if absu not in seen:
    #                     seen.add(absu)
                        
    #     elif tab_key == "7x24":
    #         tables = soup.find_all("table", class_="AnonymousHome_home__timeline-live__tb_2kb")
    #         for table in tables:
    #             for tr in table.find_all("tr"):
    #                 tds = tr.find_all("td")
    #                 if len(tds) >= 3:
    #                     link_tag = tds[2].find("a")
    #                     link = link_tag["href"]
    #                     seen.add(link)

    #     elif tab_key == "video":
    #         articles = soup.find_all("article", class_="style_timeline__item_3WW")
    #         for art in articles:
    #             link_tag = art.find("a", href=True)
    #             if link_tag:
    #                 href = link_tag["href"]
    #                 if href and href.count("/") >= 2 and href.strip("/").split("/")[-1].isdigit():
    #                     absu = urljoin(HOME_URL, href)
    #                     if absu not in seen:
    #                         seen.add(absu)

    #     return list(seen)
    async def _collect_links(self, page, tab_key: str, tab_label) -> list[str]:
        seen = set()


        try:
            # find the tab by inner text and click
            await page.click(f"text={tab_label}")
            await page.wait_for_timeout(1500)  # allow time for content to load
        except Exception as e:
            logger.warning(f"⚠️ Tab click failed for {tab_label}: {e}")
            
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        if tab_key in ["hot", "fund", "expert", "private_equity", "etf"]:
            anchors = soup.select("a.style_fake-anchor_2cg.fake-anchor[href]")
            for a in anchors:
                href = a.get("href", "")
                if href and href.count("/") >= 2 and href.strip("/").split("/")[-1].isdigit():
                    absu = urljoin(HOME_URL, href)
                    seen.add(absu)

        elif tab_key == "news":
            anchors = soup.select("a[href*='/S/']")
            for a in anchors:
                href = a.get("href", "")
                if "/S/" in href and href.count("/") >= 3:
                    absu = urljoin(HOME_URL, href)
                    seen.add(absu)

        elif tab_key == "7x24":
            tables = soup.find_all("table", class_="AnonymousHome_home__timeline-live__tb_2kb")
            for table in tables:
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) >= 3:
                        link_tag = tds[2].find("a")
                        if link_tag and link_tag.get("href"):
                            seen.add(link_tag["href"])

        elif tab_key == "video":
            articles = soup.find_all("article", class_="style_timeline__item_3WW")
            for art in articles:
                link_tag = art.find("a", href=True)
                if link_tag:
                    href = link_tag["href"]
                    if href and href.count("/") >= 2 and href.strip("/").split("/")[-1].isdigit():
                        absu = urljoin(HOME_URL, href)
                        seen.add(absu)

        return list(seen)
    # -------------------------------------------------------
    # Parse article page
    # -------------------------------------------------------
    async def _parse_article(self, context, url: str, tab_key: str) -> Optional[Dict[str, Any]]:
        page = await context.new_page()
        try:
            success = await self.safe_goto(page, url, wait="domcontentloaded")
            if not success:
                return None

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            container = soup.select_one("div.article__container") or soup
            author = author_id = None

            # --- Author ---
            author_block = container.select_one("div.article__author")
            if author_block:
                name_el = author_block.select_one(".name")
                author = name_el.get_text(strip=True)[:-2] if name_el else None
                avatar_a = author_block.select_one("a.avatar[href]")
                if avatar_a:
                    href = avatar_a.get("href")
                    if href and href.strip("/").split("/")[0].isdigit():
                        author_id = href.strip("/").split("/")[0]

            # --- Title ---
            title_el = container.select_one("h1.article__bd__title")
            title = title_el.get_text(strip=True) if title_el else None

            # --- Body ---
            body = container.select_one("div.article__bd__detail")
            text = body.get_text(separator="\n", strip=True) if body else ""
            body_html = str(body) if body else ""

            # --- Post Time ---
            post_time_el = container.select_one("time[datetime]")
            post_time = None
            if post_time_el:
                # Example: <time datetime="2025-11-09T02:16:40.000Z" title="2025-11-09 10:16">2025-11-09 10:16</time>
                post_time = post_time_el.get("datetime") or post_time_el.get_text(strip=True)

            post_id = url.strip("/").split("/")[-1]
            text_hash = _hash_text(text)

            json_info = {
                "id": post_id or text_hash,
                "url": url,
                "tab": tab_key,
                "author": author,
                "author_id": author_id,
                "title": title,
                "text": text,
                "html": body_html,
                "symbols": detect_symbols(text),
                "post_time": post_time,
                "timestamp": ts(),
            }

            return json_info


        except Exception as e:
            logger.warning(f"parse error {url}: {e}")
            return None
        finally:
            await page.close()

    # -------------------------------------------------------
    # Crawl one tab
    # -------------------------------------------------------
    async def crawl_tab(self, context, tab_key: str, tab_label_cn: str, rounds: int) -> Path:
        page = await context.new_page()
        await self._goto_tab(page, tab_label_cn)
        
        ## ALL except VIDEO
        if tab_key != "video":
            all_links = set()
            for _ in range(rounds):
                new_links = await self._collect_links(page, tab_key, tab_label_cn)
                all_links.update(new_links)
                await self._load_more(page)

            await page.close()
            results = []

            for i, url in enumerate(all_links):
                parsed = await self._parse_article(context, url, tab_key)
                if parsed:
                    results.append(parsed)
                await asyncio.sleep(0.2)
                
        ## EDIT FOR VIDEO TAB
        else:
            results = []
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Each post block
            video_blocks = soup.select("div.style_timeline__item__main_lHD")

            for block in video_blocks:
                try:
                    # --- Author ---
                    author_tag = block.select_one("a.name_name_3VM.style_user-name_Gwq")
                    author_name = author_tag.get_text(strip=True) if author_tag else None
                    author_id = author_tag.get("data-tooltip") if author_tag else None

                    # --- Post Meta (id + time) ---
                    meta_a = block.select_one("a.style_date-and-source_3r-")
                    post_href = meta_a.get("href") if meta_a else None
                    post_id = post_href.strip("/").split("/")[-1] if post_href else None
                    post_time = meta_a.get_text(strip=True).split("·")[0].replace("修改于", "").strip() if meta_a else None

                    # --- Title ---
                    title_tag = block.select_one("h3")
                    title = title_tag.get_text(strip=True) if title_tag else None

                    # --- Video link ---
                    video_tag = block.select_one("video.vjs-tech")
                    video_url = video_tag.get("src") if video_tag else None

                    # --- Symbols (optional) ---
                    symbols = []
                    for sym in block.select("a[href^='/S/']"):
                        sym_text = sym.get_text(strip=True)
                        if sym_text:
                            symbols.append(sym_text)

                    # --- Assemble record ---
                    if video_url:
                        record = {
                            "post_id": post_id,
                            "title": title,
                            "author_name": author_name,
                            "author_id": author_id,
                            "post_time": post_time,
                            "video_url": video_url,
                            "symbols": symbols,
                        }
                        results.append(record)
                except Exception as e:
                    logger.warning(f"[VIDEO] parse error: {e}")

            await page.close()
        out_path = self.raw_dir / f"posts_{tab_key}.json"
        if tab_key == "video":
            added, total = append_unique_json(out_path, results, unique_keys=("post_id",))
        else:
            added, total = append_unique_json(out_path, results)

        logger.info(f"[{tab_key}] collected={len(results)} added={added} total={total} -> {out_path}")
        return out_path




    # -------------------------------------------------------
    # Parallel master runner 
    # -------------------------------------------------------
    async def crawl(self, tab_keys: List[str], scroll_rounds: Optional[int] = None):
        rounds = scroll_rounds or self.scroll_rounds
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            context.set_default_timeout(60000)
            context.set_default_navigation_timeout(60000)

            # block unnecessary fonts/ads/analytics
            await context.route("**/*", lambda route: (
                route.abort()
                if any(x in route.request.url for x in ["fonts.googleapis.com", "analytics", "ads"])
                else route.continue_()
            ))

            tasks = []
            for key, lbl in tab_keys:
                coro = self.crawl_tab(context, key, lbl, rounds)
                tasks.append(asyncio.create_task(coro))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # log all errors
            for idx, r in enumerate(results):
                tab = tab_keys[idx] if idx < len(tab_keys) else "unknown"
                if isinstance(r, Exception):
                    logger.error(f"Task {tab} failed: {r}")
                else:
                    logger.info(f"Task {tab} finished successfully.")

            await context.close()
            await browser.close()
            return results
