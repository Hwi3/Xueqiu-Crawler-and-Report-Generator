import argparse
import asyncio
import json
import logging
from pathlib import Path
import yaml
from config import STORAGE_ROOT, default_jobname, TABS, DEFAULT_SCROLL_ROUNDS
from crawler.browser_crawler import XueqiuBrowserCrawler
from llm.summarizer import summarize_tab
from reporting.report_generator import generate_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("main")

CONFIG_PATH = Path("run_config.json")

def load_config():
    with open("run_config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

async def run(args):
    if args["job"] == "default":
        args["job"] = default_jobname()
    job_dir = STORAGE_ROOT / args["job"]
    raw_dir = job_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Running job: {job_dir}")
    for elem in args:
        logger.info(f"{elem}: {args[elem]}")
        
    

    keys = list(TABS.values())
    if args["tabs"] != "all":
        # Check argument tabs are all valid
        
        tab_keys = []
        requested = [k.strip() for k in args["tabs"].split(",")]
        for (name_cn, (k, lbl)) in TABS.items():
            if k in requested:
                tab_keys.append((k, lbl))
        if not tab_keys:
            raise ValueError(f"No valid tabs found in request: {args['tabs']}")
    else:
        tab_keys = [v for (k, v) in TABS.items()]

    if args["mode"] in ("crawl", "all"):
        logger.info("[1/3] Start crawling...")
        logger.info(f"Tabs to crawl: {[k for (k, _) in tab_keys]}") 
        crawler = XueqiuBrowserCrawler(raw_dir, scroll_rounds=args["scroll"])
        await crawler.crawl(tab_keys)
        logger.info("[1/3] Crawling Done.")

    if args["mode"] in ("summarize", "all"):
        logger.info("[2/3] Start summarizing...")
        for k,_  in tab_keys:
            summarize_tab(job_dir, k, args["sum_limit"])
        logger.info("[2/3] Summarizing Done.")

    if args["mode"] in ("report", "all"):
        logger.info("[3/3] Generating report...")
        p = generate_report(job_dir, args["job"])
        logger.info(f"[3/3] Report saved at: {p}")


def main():
    cfg = load_config()
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
