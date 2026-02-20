# scripts/run_selected_odds.py
# Purpose:
# - Read input/Over15.csv and input/Over05_1h.csv
# - For each match, try to find a VALID OddsPortal match page via search
# - NEVER output fake links. If not validated -> leave blank and mark link-miss.
# - Save out/match_links.csv + debug screenshots

from __future__ import annotations

import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


INPUT_OVER15 = Path("input/Over15.csv")
INPUT_OVER05_1H = Path("input/Over05_1h.csv")
OUT_DIR = Path("out")
DEBUG_DIR = OUT_DIR / "debug"
OUT_MATCH_LINKS = OUT_DIR / "match_links.csv"

# ---- Helpers ----

def _slug(s: str) -> str:
    s = s.lower().strip()
    # normalize common separators
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    # keep letters/numbers/spaces only
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return s.strip()

def _split_match(match: str) -> Tuple[str, str]:
    # Expect "Home vs Away"
    if " vs " in match:
        a, b = match.split(" vs ", 1)
        return a.strip(), b.strip()
    # fallback
    parts = match.split("v", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return match.strip(), ""

def _read_input_csv(path: Path) -> List[Tuple[int, str, str]]:
    if not path.exists():
        return []
    rows: List[Tuple[int, str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        # Try DictReader first (expects header idx,match,league)
        sample = f.read(4096)
        f.seek(0)
        has_header = "match" in sample.splitlines()[0].lower()
        if has_header:
            r = csv.DictReader(f)
            for line in r:
                try:
                    idx = int(str(line.get("idx", "")).strip() or "0")
                except ValueError:
                    idx = 0
                match = (line.get("match") or "").strip()
                league = (line.get("league") or "").strip()
                if idx and match:
                    rows.append((idx, match, league))
        else:
            # Fallback: assume format idx,match,league without header
            r2 = csv.reader(f)
            for parts in r2:
                if len(parts) < 2:
                    continue
                try:
                    idx = int(parts[0].strip())
                except ValueError:
                    continue
                match = parts[1].strip()
                league = parts[2].strip() if len(parts) >= 3 else ""
                if idx and match:
                    rows.append((idx, match, league))
    return rows

def _timestamp() -> str:
    return time.strftime("%Y%m%d_%H%M%S")

@dataclass
class MatchTask:
    bucket: str
    idx: int
    match: str
    league: str
    oddsportal_link: str = ""   # empty unless validated
    status: str = "pending"     # ok / link-miss

# ---- OddsPortal logic ----

ODDSPORTAL_BASE = "https://www.oddsportal.com"

def _accept_cookies_if_present(page) -> None:
    # Try several common buttons. If not present, ignore.
    candidates = [
        "button:has-text('I Accept')",
        "button:has-text('Accept')",
        "button:has-text('Accept All')",
        "text=I Accept",
        "text=Accept All",
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel).first
            if btn and btn.is_visible(timeout=800):
                btn.click(timeout=800)
                page.wait_for_timeout(300)
                return
        except Exception:
            pass

def _close_overlays(page) -> None:
    # Sometimes there is a privacy modal covering the page
    # Try ESC and some close buttons
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    for sel in ["button[aria-label='Close']", "text=Close", "text=Reject All"]:
        try:
            el = page.locator(sel).first
            if el and el.is_visible(timeout=600):
                el.click(timeout=600)
                page.wait_for_timeout(250)
                return
        except Exception:
            pass

def _search_and_pick_match_link(page, home: str, away: str, league: str) -> Optional[str]:
    """
    Strategy:
    1) Open OddsPortal search page with query: "Home Away"
    2) Accept cookies if needed
    3) Find search results that look like a MATCH page
    4) Validate by opening candidate and checking both team names appear in page text
    Returns match URL or None.
    """
    query = f"{home} {away}".strip()
    search_url = f"{ODDSPORTAL_BASE}/search/?q={re.sub(r'\\s+', '+', query)}"
    page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(700)
    _accept_cookies_if_present(page)
    _close_overlays(page)
    page.wait_for_timeout(500)

    # Collect candidate links
    # OddsPortal search results can vary; we grab all internal links and filter.
    anchors = page.locator("a[href^='/']").all()
    hrefs: List[str] = []
    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
            if not href:
                continue
            # avoid obvious non-match sections
            if href.startswith("/search/"):
                continue
            # Often match pages contain "/football/" and end with a slash.
            if "/football/" in href and href.count("/") >= 4:
                full = ODDSPORTAL_BASE + href
                if full not in hrefs:
                    hrefs.append(full)
        except Exception:
            continue

    # If nothing, return None
    if not hrefs:
        return None

    # Validate candidates by checking page text has both teams
    home_s = _slug(home)
    away_s = _slug(away)

    for cand in hrefs[:8]:  # limit attempts
        try:
            page.goto(cand, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(700)
            _accept_cookies_if_present(page)
            _close_overlays(page)
            page.wait_for_timeout(600)
            txt = page.inner_text("body", timeout=3000)
            t = _slug(txt)
            if home_s and away_s and (home_s in t) and (away_s in t):
                return cand
        except Exception:
            continue

    return None

# ---- Main ----

def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    tasks: List[MatchTask] = []
    for idx, match, league in _read_input_csv(INPUT_OVER15):
        tasks.append(MatchTask(bucket="over15", idx=idx, match=match, league=league))
    for idx, match, league in _read_input_csv(INPUT_OVER05_1H):
        tasks.append(MatchTask(bucket="over05_1h", idx=idx, match=match, league=league))

    if not tasks:
        print("[warn] No tasks found in input CSVs.")
        return 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1400, "height": 900})
        page = context.new_page()

        for tsk in tasks:
            home, away = _split_match(tsk.match)
            try:
                link = _search_and_pick_match_link(page, home, away, tsk.league)
                if link:
                    tsk.oddsportal_link = link
                    tsk.status = "ok"
                    print(f"[ok] {tsk.bucket} #{tsk.idx} {tsk.match} ({tsk.league}) -> {link}")
                else:
                    tsk.status = "link-miss"
                    print(f"[link-miss] {tsk.bucket} #{tsk.idx} {tsk.match} ({tsk.league})")
                    # Debug screenshot
                    try:
                        page.screenshot(path=str(DEBUG_DIR / f"{_timestamp()}_search-miss-{tsk.bucket}-{tsk.idx}.png"), full_page=True)
                    except Exception:
                        pass
            except PWTimeoutError:
                tsk.status = "link-miss"
                print(f"[timeout] {tsk.bucket} #{tsk.idx} {tsk.match} ({tsk.league})")
                try:
                    page.screenshot(path=str(DEBUG_DIR / f"{_timestamp()}_timeout-{tsk.bucket}-{tsk.idx}.png"), full_page=True)
                except Exception:
                    pass
            except Exception as e:
                tsk.status = "link-miss"
                print(f"[error] {tsk.bucket} #{tsk.idx} {tsk.match} ({tsk.league}) -> {e}")
                try:
                    page.screenshot(path=str(DEBUG_DIR / f"{_timestamp()}_error-{tsk.bucket}-{tsk.idx}.png"), full_page=True)
                except Exception:
                    pass

        context.close()
        browser.close()

    # Write output CSV
    with OUT_MATCH_LINKS.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["bucket", "idx", "match", "league", "oddsportal_link", "status"])
        for t in tasks:
            w.writerow([t.bucket, t.idx, t.match, t.league, t.oddsportal_link, t.status])

    print(f"[ok] wrote: {OUT_MATCH_LINKS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
