#!/usr/bin/env python3
"""
Run selected odds scrape (Over 1.5 FT + Over 0.5 1H) based on input CSVs.

NOTA:
- En esta iteración dejamos el scraping REAL como “diagnóstico” para confirmar
  la estructura exacta del JSON/keys que devuelve oddsharvester, porque cambia
  según market/period/bookie.
- El objetivo de este archivo es: 1) existir en la ruta correcta, 2) leer inputs,
  3) ejecutar un scrape mínimo y 4) guardar un JSON crudo para que ya sin adivinar
  hagamos el mapeo final de momios.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from oddsharvester.core.scraper_app import run_scraper
from oddsharvester.utils.command_enum import CommandEnum


ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "input"
OUT_DIR = ROOT / "out"

OVER15_CSV = INPUT_DIR / "Over15.csv"
OVER05_1H_CSV = INPUT_DIR / "Over05_1h.csv"

# bookies (por ahora 1) -> en el siguiente paso lo hacemos 2 y promedio
TARGET_BOOKMAKER = os.environ.get("TARGET_BOOKMAKER", "bet365.us")


def _tomorrow_yyyymmdd_utc() -> str:
    now = datetime.now(timezone.utc)
    tomorrow = now + timedelta(days=1)
    return tomorrow.strftime("%Y%m%d")


def _read_input_csv(path: Path) -> list[dict[str, str]]:
    """
    Lee tu CSV que trae líneas como:
    Filtro:,Over 1.5,
    ,,
    1,Tigres UANL vs Pachuca CF,México Liga MX
    ...
    Retorna lista de {idx, match, league}.
    """
    rows: list[dict[str, str]] = []
    if not path.exists():
        raise FileNotFoundError(f"No existe: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            if not r:
                continue
            # Queremos filas tipo: [num, "Equipo A vs Equipo B", "Liga..."]
            if len(r) < 3:
                continue
            idx = (r[0] or "").strip()
            match = (r[1] or "").strip()
            league = (r[2] or "").strip()

            if not idx.isdigit():
                continue
            if " vs " not in match:
                continue

            rows.append({"idx": idx, "match": match, "league": league})

    return rows


async def _scrape_raw_over15(date_yyyymmdd: str) -> dict[str, Any]:
    """
    Scrape “crudo” para Over 1.5 FT del día (por fecha).
    En el siguiente paso: filtramos por tus inputs y extraemos momios por bookie.
    """
    # Importante: headless True en GH Actions
    result = await run_scraper(
        command=CommandEnum.UPCOMING_MATCHES,
        sport="football",
        date=date_yyyymmdd,
        leagues=None,
        markets=["over_under_1_5"],
        target_bookmaker=TARGET_BOOKMAKER,
        headless=True,
        preview_submarkets_only=False,
        period=None,
    )

    if result is None:
        return {"ok": False, "error": "run_scraper devolvió None"}

    # ScrapeResult es dataclass-like con .success/.failed/.stats
    payload: dict[str, Any] = {
        "ok": True,
        "target_bookmaker": TARGET_BOOKMAKER,
        "date": date_yyyymmdd,
        "stats": getattr(result, "stats", None).__dict__ if getattr(result, "stats", None) else None,
        "success_count": len(getattr(result, "success", []) or []),
        "failed_count": len(getattr(result, "failed", []) or []),
        "success": getattr(result, "success", []),
        "failed": [f.__dict__ for f in getattr(result, "failed", [])] if getattr(result, "failed", None) else [],
    }
    return payload


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    over15 = _read_input_csv(OVER15_CSV)
    over05 = _read_input_csv(OVER05_1H_CSV)

    print(f"[OK] Inputs leídos:")
    print(f" - Over15: {len(over15)} filas")
    print(f" - Over0.5 1H: {len(over05)} filas")

    date = _tomorrow_yyyymmdd_utc()
    print(f"[OK] Fecha objetivo (UTC tomorrow): {date}")
    print(f"[OK] Target bookmaker: {TARGET_BOOKMAKER}")

    # Scrape crudo (para diagnóstico de estructura)
    import asyncio  # noqa

    raw = asyncio.run(_scrape_raw_over15(date))

    raw_path = OUT_DIR / "raw_over15.json"
    raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Guardado: {raw_path}")

    # También guardo “muestra” de keys para que veas qué trae cada match
    sample_path = OUT_DIR / "raw_over15_sample_keys.txt"
    lines: list[str] = []
    success = raw.get("success") or []
    for i, item in enumerate(success[:25], 1):
        if isinstance(item, dict):
            lines.append(f"#{i} keys: {sorted(list(item.keys()))}")
    sample_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] Guardado: {sample_path}")

    print("[DONE] Paso diagnóstico completo.")


if __name__ == "__main__":
    main()
