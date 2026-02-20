#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE 1 (ONLY): Find the correct OddsPortal match page link for each input row.

Input:
- input/Over15.csv
- input/Over05_1h.csv

Output:
- out/match_links.csv with columns:
  bucket, idx, match, league, oddsportal_link, status

Key rules:
- Skip non-data rows (like "Filtro:,Over 1.5," or empty rows).
- Use site search (top search) and pick the best result by:
  - both team names present
  - league text similarity (your league column helps)
- If not confidently found -> leave oddsportal_link blank and status=link-miss
- NO fallback to generic competition URLs.
"""

from __future__ import annotations

import asyncio
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple

from playwright.async_api import async_playwright, Page


INPUT_OVER15 = Path("input/Over15.csv")
INPUT_OVER05_1H = Path("input/Over05_1h.csv")
OUT_DIR = Path("out")
OUT_LINKS = OUT_DIR / "match_links.csv"


# -------------------------
# Normalization helpers
# -------------------------

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def norm(s: str) -> str:
    s = norm_space(s).lower()
    s = re.sub(r"[’'`´\.\,\(\)\[\]\-_/]", " ", s)
    return norm_space(s)

def split_match(match: str) -> Tuple[str, str]:
    # expects "Team A vs Team B"
    parts = match.split(" vs ")
    if len(parts) != 2:
        return norm(match), ""
    return norm(parts[0]), norm(parts[1])

def looks_like_int(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", (s or "").strip()))

def safe_get(row: List[str], i: int) -> str:
    return row[i].strip() if i < len(row) and row[i] is not None else ""


@dataclass
class Row:
    bucket: str
    idx: int
    match: str
    league: str


def read_input_csv(path: Path, bucket: str) -> List[Row]:
    rows: List[Row] = []
    if not path.exists():
        print(f"[warn] missing input file: {path}")
        return rows

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for raw in reader:
            # expected columns in your export:
            # A: idx, B: match, C: league
            # but you also have junk lines like "Filtro:,Over 1.5," and ",,"
            a = safe_get(raw, 0)
            b = safe_get(raw, 1)
            c = safe_get(raw, 2)

            if not looks_like_int(a):
                continue
            if not b or " vs " not in b:
                continue

            rows.append(Row(bucket=bucket, idx=int(a), match=b.strip(), league=c.strip()))
    return rows


# -------------------------
# OddsPortal automation
# -------------------------

async def accept_cookies_if_present(page: Page) -> None:
    # OddsPortal often shows "I Accept" button.
    # We'll try a few common locators; if not present, ignore.
    candidates = [
        page.get_by_role("button", name=re.compile(r"i accept", re.I)),
        page.get_by_text(re.compile(r"\bI Accept\b", re.I)),
        page.locator("button:has-text('I Accept')"),
    ]
    for loc in candidates:
        try:
            if await loc.count() > 0:
                await loc.first.click(timeout=2000)
                await page.wait_for_timeout(300)
                return
        except Exception:
            pass


async def open_search(page: Page, query: str) -> None:
    # Use the top search (magnifier). We avoid brittle CSS selectors.
    # Steps:
    # - open homepage
    # - accept cookies if needed
    # - click search icon
    # - type query and press Enter
    await page.goto("https://www.oddsportal.com/", wait_until="domcontentloaded")
    await accept_cookies_if_present(page)

    # Click search icon (magnifier). Several versions exist.
    # We'll try multiple fallbacks.
    clicked = False
    for selector in [
        "button[aria-label*='Search' i]",
        "a[aria-label*='Search' i]",
        "svg[aria-label*='Search' i]",
        "text=Search",
        "css=header >> text=Search",
        "css=.icon-search",
    ]:
        try:
            loc = page.locator(selector)
            if await loc.count() > 0:
                await loc.first.click(timeout=2500)
                clicked = True
                break
        except Exception:
            continue

    # If we couldn't click, we still try to focus any visible input.
    # Now locate an input and type.
    inp = page.locator("input[type='search'], input[placeholder*='Search' i], input[name*='search' i]")
    await inp.first.wait_for(timeout=7000)
    await inp.first.fill(query)
    await inp.first.press("Enter")


async def collect_search_results(page: Page) -> List[Tuple[str, str]]:
    """
    Return list of (title_text, href) from search results.
    We try multiple layouts.
    """
    await page.wait_for_load_state("domcontentloaded")
    await accept_cookies_if_present(page)
    await page.wait_for_timeout(300)

    results: List[Tuple[str, str]] = []

    # Common pattern: links in a result list
    link_locs = [
        page.locator("a[href*='/football/']").filter(has_text=re.compile(r".+", re.S)),
        page.locator("a[href*='/match/']").filter(has_text=re.compile(r".+", re.S)),
    ]

    seen = set()
    for loc in link_locs:
        try:
            n = await loc.count()
            for i in range(min(n, 40)):
                a = loc.nth(i)
                href = await a.get_attribute("href")
                txt = norm_space((await a.inner_text()) or "")
                if not href or not txt:
                    continue
                if href.startswith("/"):
                    href_full = "https://www.oddsportal.com" + href
                else:
                    href_full = href
                key = (txt, href_full)
                if key in seen:
                    continue
                seen.add(key)
                results.append(key)
        except Exception:
            continue

    return results


def score_candidate(match: str, league: str, cand_text: str) -> int:
    """
    Simple scoring:
    - requires both team names present (strong)
    - adds points if league words overlap
    """
    h, a = split_match(match)
    t = norm(cand_text)

    if not h or not a:
        return 0

    if h not in t or a not in t:
        return 0

    score = 100  # base for both teams present

    # League overlap bonus
    lg = norm(league)
    if lg:
        lg_words = [w for w in lg.split(" ") if len(w) >= 4]
        hits = sum(1 for w in lg_words if w in t)
        score += min(30, hits * 6)

    return score


async def find_match_link(page: Page, row: Row) -> Tuple[str, str]:
    """
    Returns (oddsportal_link, status)
    status: ok | link-miss
    """
    query = row.match.replace(" vs ", " ")
    await open_search(page, query)

    results = await collect_search_results(page)

    best_href = ""
    best_score = 0
    best_text = ""

    for txt, href in results:
        sc = score_candidate(row.match, row.league, txt)
        if sc > best_score:
            best_score = sc
            best_href = href
            best_text = txt

    # Confidence threshold:
    # 100 means both teams matched; we accept that.
    # If it is 0 -> no match.
    if best_score >= 100 and best_href:
        return best_href, "ok"

    # no confident match
    return "", "link-miss"


async def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    rows += read_input_csv(INPUT_OVER15, "over15")
    rows += read_input_csv(INPUT_OVER05_1H, "over05_1h")

    print(f"[info] loaded rows: {len(rows)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        )
        page = await context.new_page()

        out = []
        for r in rows:
            try:
                link, status = await find_match_link(page, r)
                print(f"[{status}] {r.bucket} #{r.idx} {r.match} ({r.league}) -> {link}")
                out.append((r.bucket, r.idx, r.match, r.league, link, status))
            except Exception as e:
                print(f"[link-miss] {r.bucket} #{r.idx} {r.match} ({r.league}) -> error: {e}")
                out.append((r.bucket, r.idx, r.match, r.league, "", "link-miss"))

        await context.close()
        await browser.close()

    with OUT_LINKS.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["bucket", "idx", "match", "league", "oddsportal_link", "status"])
        w.writerows(out)

    print(f"[ok] wrote: {OUT_LINKS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
