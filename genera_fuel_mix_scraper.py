#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from datetime import datetime, timezone
import csv, json, sys, argparse

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

URL = "https://genera-pr.com/data-generacion/"
A11Y_TABLE = 'div[aria-label*="tabular representation"] table'
CSV_PATH = Path("fuel_mix.csv")

def extract_rows_from_tables(page):
    rows = []
    tables = page.locator(A11Y_TABLE)
    tcount = tables.count()
    for ti in range(tcount):
        tbody_rows = tables.nth(ti).locator("tbody tr")
        rc = tbody_rows.count()
        for ri in range(rc):
            tds = tbody_rows.nth(ri).locator("td")
            if tds.count() < 2:
                continue
            fuel = tds.nth(0).inner_text().strip()
            pct_raw = tds.nth(1).inner_text().strip()
            try:
                pct = float(pct_raw.replace("%", "").strip())
            except Exception:
                pct = pct_raw
            if fuel:
                rows.append((fuel, pct))
    return rows

def main():
    ap = argparse.ArgumentParser(description="Scrape Genera PR fuel-mix table")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--timeout", type=int, default=60_000)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=not args.headed)
            context = browser.new_context()
            page = context.new_page()

            # SAFE console logger (properties, not callables)
            if args.verbose:
                def _log_console(msg):
                    try:
                        print(f"[PAGE:{msg.type}] {msg.text}", file=sys.stderr, flush=True)
                    except Exception as e:
                        print(f"[PAGE][console-log-error] {e}", file=sys.stderr, flush=True)
                page.on("console", _log_console)

            page.goto(URL, wait_until="domcontentloaded", timeout=args.timeout)

            # Give charts a moment to render their a11y table
            try:
                page.wait_for_selector(A11Y_TABLE, timeout=30_000)
            except PwTimeout:
                # Some pages inject late; wait a bit more
                page.wait_for_timeout(5000)

            data = extract_rows_from_tables(page)

            if not data:
                # dump HTML for debugging
                Path("fuel_mix_debug.html").write_text(page.content(), encoding="utf-8")
                print("[ERROR] No rows found. Saved fuel_mix_debug.html", file=sys.stderr)
                sys.exit(2)

            ts = datetime.now(timezone.utc).isoformat()
            write_header = not CSV_PATH.exists()
            with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if write_header:
                    w.writerow(["timestamp_utc", "fuel", "percent"])
                for fuel, pct in data:
                    w.writerow([ts, fuel, pct])

            summary = {"timestamp_utc": ts, "rows": [{"fuel": f, "percent": p} for f, p in data]}
            print(json.dumps(summary, ensure_ascii=False), flush=True)

            context.close()
            browser.close()

    except PwTimeout:
        print("[ERROR] Timeout while loading or waiting for selectors.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Uncaught exception: {e}", file=sys.stderr)
        sys.exit(99)

if __name__ == "__main__":
    main()
