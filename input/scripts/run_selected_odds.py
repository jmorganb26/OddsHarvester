name: Scrape odds tomorrow

on:
  workflow_dispatch:

jobs:
  scrape:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install uv
          uv sync

      - name: Smoke test - load selected universes
        run: |
          python scripts/run_selected_odds.py
