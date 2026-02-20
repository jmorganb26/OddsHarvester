import asyncio
import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

ODDSPORTAL_BASE = "https://www.oddsportal.com"
SPORT = "football"

INPUT_OVER15 = Path("input/Over15.csv")
INPUT_OVER05_1H = Path("input/Over05_1h.csv")

OUT_DIR = Path("out")
DEBUG_DIR = OUT_DIR / "debug"


def env_true(name: str) -> bool:
    v = os.getenv(name, "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


HEADLESS = env_true("OH_HEADLESS")
DEBUG_DUMP = env_true("OH_DEBUG_DUMP")


@dataclass
class Row:
    idx: str
    match: str
    league: str


def slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:80] or "item"


def read_input_csv(path: Path) -> list[Row]:
    rows: list[Row] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            if not r:
                continue
            # Tus CSV traen encabezados tipo: "Filtro:,Over 1.5," y luego ",,"
            if len(r) >= 1 and (r[0].startswith("Filtro:") or r[0].strip() == ""):
                continue
            # Formato esperado: idx, match, league
            if len(r) < 3:
                continue
            idx, match, league = r[0].strip(), r[1].strip(), r[2].strip()
            if not idx.isdigit():
                continue
            if not match or " vs " not in match:
                continue
            rows.append(Row(idx=idx, match=match, league=league))
    return rows


def normalize_team_name(s: str) -> str:
    s = s.lower().strip()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ü", "u").replace("ñ", "n")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def split_match(match: str) -> tuple[str, str]:
    home, away = match.split(" vs ", 1)
    return normalize_team_name(home), normalize_team_name(away)


async def dump_debug(page, tag: str):
    if not DEBUG_DUMP:
        return
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = DEBUG_DIR / f"{ts}_{slug(tag)}"
    try:
        await page.screenshot(path=str(base) + ".png", full_page=True)
    except Exception:
        pass
    try:
        html = await page.content()
        (Path(str(base) + ".html")).write_text(html, encoding="utf-8")
    except Exception:
        pass


async def find_match_link_for_row(page, row: Row) -> Optional[str]:
    """
    Estrategia:
    1) Buscar por el match completo (texto) en OddsPortal (search).
    2) Abrir resultados y escoger el que mejor matchee por home/away.
    """
    home, away = split_match(row.match)

    query = row.match
    search_url = f"{ODDSPORTAL_BASE}/search/?q={query.replace(' ', '+')}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(800)

    # Si OddsPortal cambia el DOM, esto igual puede fallar: por eso debug dump.
    try:
        items = page.locator("a").filter(has_text=" - ").all()
    except Exception:
        items = []

    # Fallback más robusto: buscar anchors que contengan ambos equipos (normalizados)
    anchors = await page.query_selector_all("a[href]")
    best = None

    for a in anchors:
        try:
            text = (await a.inner_text()) or ""
            t = normalize_team_name(text)
            if home and away and (home in t) and (away in t):
                href = await a.get_attribute("href")
                if href and "/match/" in href:
                    best = href
                    break
        except Exception:
            continue

    if not best:
        return None

    if best.startswith("/"):
        return ODDSPORTAL_BASE + best
    if best.startswith("http"):
        return best
    return ODDSPORTAL_BASE + "/" + best.lstrip("/")


async def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    over15_rows = read_input_csv(INPUT_OVER15)
    over05_rows = read_input_csv(INPUT_OVER05_1H)

    print(f"[input] Over1.5 rows: {len(over15_rows)}")
    print(f"[input] Over0.5 1H rows: {len(over05_rows)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context()
        page = await context.new_page()

        # 1) Resolver links
        link_map: dict[str, str] = {}

        async def resolve(rows: list[Row], label: str):
            for r in rows:
                key = f"{label}:{r.idx}"
                try:
                    link = await find_match_link_for_row(page, r)
                    if not link:
                        print(f"[link-miss] {label} #{r.idx} {r.match} ({r.league})")
                        await dump_debug(page, f"search_miss_{label}_{r.idx}_{r.match}")
                        continue
                    link_map[key] = link
                    print(f"[link-ok] {label} #{r.idx} -> {link}")
                except Exception as e:
                    print(f"[link-error] {label} #{r.idx} {r.match}: {e}")
                    await dump_debug(page, f"search_err_{label}_{r.idx}_{r.match}")

        await resolve(over15_rows, "over15")
        await resolve(over05_rows, "over05_1h")

        await browser.close()

    # Guardamos el mapping para que veas qué encontró y qué no
    mapping_path = OUT_DIR / "match_links.csv"
    with mapping_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["bucket", "idx", "match", "league", "oddsportal_link"])
        for r in over15_rows:
            k = f"over15:{r.idx}"
            w.writerow(["over15", r.idx, r.match, r.league, link_map.get(k, "")])
        for r in over05_rows:
            k = f"over05_1h:{r.idx}"
            w.writerow(["over05_1h", r.idx, r.match, r.league, link_map.get(k, "")])

    print(f"[ok] wrote: {mapping_path}")


if __name__ == "__main__":
    asyncio.run(main())
