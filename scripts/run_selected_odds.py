import asyncio
import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page


ODDSPORTAL_BASE = "https://www.oddsportal.com"
OUT_DIR = Path("out")
DEBUG_DIR = OUT_DIR / "debug"
INPUT_DIR = Path("input")

# Input files
IN_OVER15 = INPUT_DIR / "Over15.csv"
IN_OVER05_1H = INPUT_DIR / "Over05_1h.csv"

# Output
OUT_MATCH_LINKS = OUT_DIR / "match_links.csv"


@dataclass
class InputRow:
    bucket: str            # over15 / over05_1h
    idx: str               # original index from your sheet
    match: str             # "Team A vs Team B"
    league: str            # "México Liga MX" etc.


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def read_input_csv(path: Path, bucket: str) -> list[InputRow]:
    """
    Reads your CSV exported from Sheets.
    Expected rows like:
      1,Tigres UANL vs Pachuca CF,México Liga MX
    It may also have extra header lines like:
      Filtro:,Over 1.5,
      ,,
    We skip rows that don't look like (idx, match, league).
    """
    rows: list[InputRow] = []
    if not path.exists():
        return rows

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            if not r:
                continue
            # We expect at least 3 columns
            if len(r) < 3:
                continue

            idx = (r[0] or "").strip()
            match = (r[1] or "").strip()
            league = (r[2] or "").strip()

            # Skip header-ish rows
            if not idx.isdigit():
                continue
            if " vs " not in match.lower():
                continue

            rows.append(InputRow(bucket=bucket, idx=idx, match=match, league=league))

    return rows


def normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[\u2019\u2018\u201C\u201D]", "'", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


async def accept_privacy_if_present(page: Page) -> None:
    """
    OddsPortal often shows a privacy/cookie overlay with buttons like:
    "I Accept", "Accept All", "Reject All".
    This function tries multiple safe selectors.
    """
    candidates = [
        re.compile(r"^i accept$", re.I),
        re.compile(r"^accept all$", re.I),
        re.compile(r"^accept$", re.I),
        re.compile(r"^agree$", re.I),
    ]

    # Try up to a few times because the overlay can render late
    for _ in range(5):
        try:
            for patt in candidates:
                btn = page.get_by_role("button", name=patt)
                if await btn.count() > 0:
                    # Click the first visible one
                    try:
                        await btn.first.click(timeout=1200)
                        await page.wait_for_timeout(400)
                        return
                    except Exception:
                        pass
        except Exception:
            pass
        await page.wait_for_timeout(500)


async def search_match_on_oddsportal(page: Page, match: str) -> None:
    """
    Uses OddsPortal top search (magnifier) by opening a search URL.
    This avoids needing brittle UI selectors for the search box.
    """
    q = match.strip()
    # OddsPortal has /search/ which redirects to Next Match Search results.
    url = f"{ODDSPORTAL_BASE}/search/?q={q.replace(' ', '%20')}"
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    await accept_privacy_if_present(page)
    await page.wait_for_timeout(600)


async def extract_first_match_link(page: Page, wanted_match: str) -> Optional[str]:
    """
    On the search results page, try to find a reasonable football match link.
    We prefer links that contain both teams words, but keep it simple/stable.
    """
    wanted_norm = normalize_text(wanted_match)

    # Pull all anchors; filter likely event links
    anchors = page.locator("a[href]")
    n = await anchors.count()
    if n == 0:
        return None

    best_href = None
    best_score = 0

    for i in range(min(n, 400)):  # cap to avoid huge loops
        a = anchors.nth(i)
        try:
            href = await a.get_attribute("href")
            if not href:
                continue

            # Typical match URLs contain /football/... and have long-ish paths
            if "/football/" not in href:
                continue
            if "/results/" in href:
                continue

            text = (await a.inner_text()) or ""
            text_norm = normalize_text(text)

            # Score by token overlap with wanted match
            score = 0
            for token in wanted_norm.split():
                if token and token in text_norm:
                    score += 1

            if score > best_score:
                best_score = score
                best_href = href

            # Early exit if it's very likely the right one
            if best_score >= 4:
                break
        except Exception:
            continue

    if not best_href:
        return None

    if best_href.startswith("http"):
        return best_href
    return f"{ODDSPORTAL_BASE}{best_href}"


async def run() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    over15 = read_input_csv(IN_OVER15, "over15")
    over05 = read_input_csv(IN_OVER05_1H, "over05_1h")
    all_rows = over15 + over05

    if not all_rows:
        print("[error] No input rows found. Check input/Over15.csv and input/Over05_1h.csv")
        return 2

    print(f"[info] rows: over15={len(over15)} over05_1h={len(over05)} total={len(all_rows)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        )
        page = await context.new_page()

        out_rows = []
        for r in all_rows:
            try:
                await search_match_on_oddsportal(page, r.match)
                link = await extract_first_match_link(page, r.match)

                if not link:
                    # Debug screenshot
                    shot = DEBUG_DIR / f"{_now_utc_str()}_search-miss-{r.bucket}-{r.idx}.png"
                    try:
                        await page.screenshot(path=str(shot), full_page=True)
                    except Exception:
                        pass
                    print(f"[link-miss] {r.bucket} #{r.idx} {r.match} ({r.league})")
                else:
                    print(f"[ok] {r.bucket} #{r.idx} -> {link}")

                out_rows.append({
                    "bucket": r.bucket,
                    "idx": r.idx,
                    "match": r.match,
                    "league": r.league,
                    "oddsportal_link": link or "",
                })

            except Exception as e:
                shot = DEBUG_DIR / f"{_now_utc_str()}_error-{r.bucket}-{r.idx}.png"
                try:
                    await page.screenshot(path=str(shot), full_page=True)
                except Exception:
                    pass
                print(f"[error] {r.bucket} #{r.idx} {r.match}: {e}")
                out_rows.append({
                    "bucket": r.bucket,
                    "idx": r.idx,
                    "match": r.match,
                    "league": r.league,
                    "oddsportal_link": "",
                })

        await context.close()
        await browser.close()

    # Write output CSV
    with OUT_MATCH_LINKS.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["bucket", "idx", "match", "league", "oddsportal_link"])
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"[done] wrote: {OUT_MATCH_LINKS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
