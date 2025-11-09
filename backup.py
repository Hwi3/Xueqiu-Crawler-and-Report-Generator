
    # -------------------------------------------------------
    # Crawl 7x24
    # -------------------------------------------------------
    async def crawl_7x24(self,context,rounds) -> List[Dict[str, Any]]:
        page = await context.new_page()
        await self._goto_tab(page, "7x24")
        timeline_json_list = ()
        for _ in range(rounds):
            json_links = 
            
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
 elif tab_key == "7x24":
            tables = soup.find_all("table", class_="AnonymousHome_home__timeline-live__tb_2kb")
            rows = []
            for table in tables:
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) >= 3:
                        time = tds[0].text.strip()
                        link_tag = tds[2].find("a")
                        link = link_tag["href"]
                        text = link_tag.text.strip()
                        rows.append({"time": time, "link": link, "text": text})
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
            
            
            
            out_path = self.raw_dir / f"posts_7x24.json"
            results = []
            for n in nodes:
                try:
                    raw_html = str(n) 
                    text = n.get_text(separator="\n", strip=True)
                    # author
                    author = None 
                    a = n.select_one("a.name_name_3VM, a.style_user-name_Gwq") 
                    if a and a.get_text(strip=True): 
                        author = a.get_text(strip=True) 
                    # link/id 
                    link = n.select_one("a[href]") 
                    post_id = None
                    if link and link.get("href"): 
                        # e.g., /9887656769/360383848
                        parts = link.get("href").strip("/").split("/") 
                        if parts and parts[-1].isdigit():
                            post_id = parts[-1]
                    text_hash = _hash_text(text)
                    # skip empties
                    if not text and not raw_html:
                        continue
                    result_json = { "id": post_id or text_hash,
                                "text": text,
                                "html": raw_html,
                                "timestamp": ts(),
                                "tab": "7x24",
                                "author": author,
                                "symbols": detect_symbols(text),
                                "text_hash": text_hash, }
                    results.append(result_json)
                except Exception as e:
                    logger.warning(f"extract node error: {e}")
                    
                added, total = append_unique_json(out_path, results)
                
                logger.info(f"[7x24] collected={len(results)} added={added} total={total} -> {out_path}")
                return out_path



            if tab_key == "7x24":
                time_el = soup.find("time", class_="datetime")
                if time_el:
                    json_info["time"] = time_el.get_text(strip=True)