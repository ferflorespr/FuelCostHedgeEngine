#!/usr/bin/env python3
import argparse, csv, datetime as dt, json, os, sqlite3, sys, time
from pathlib import Path

COLUMNS = [
    "scraped_at_iso",
    "updated_label_raw",
    "generacion_total_MW",
    "capacidad_disponible_MW",
    "reserva_en_rotacion_MW",
    "reserva_operacional_MW",
    "demanda_proxima_hora_MW",
    "demanda_maxima_hoy_MW",
    "gas_natural_pct",
    "carbon_pct",
    "bunker_diesel_pct",
    "renovables_pct",
    "otros_pct",
    "source_pages",
]

def ensure_csv(csv_path: Path):
    exists = csv_path.exists()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not exists:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(COLUMNS)

def ensure_sqlite(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            scraped_at_iso TEXT PRIMARY KEY,
            updated_label_raw TEXT,
            generacion_total_MW REAL,
            capacidad_disponible_MW REAL,
            reserva_en_rotacion_MW REAL,
            reserva_operacional_MW REAL,
            demanda_proxima_hora_MW REAL,
            demanda_maxima_hoy_MW REAL,
            gas_natural_pct REAL,
            carbon_pct REAL,
            bunker_diesel_pct REAL,
            renovables_pct REAL,
            otros_pct REAL,
            source_pages TEXT
        )
    """)
    conn.commit()
    conn.close()

def parse_row(payload: dict) -> list:
    # timestamps
    scraped_at = payload.get("scraped_at")  # e.g., "2025-10-20T23:31:15-04:00"
    # Normalize to ISO8601 Z if possible
    try:
        scraped_dt = dt.datetime.fromisoformat(scraped_at)
        scraped_at_iso = scraped_dt.isoformat()
    except Exception:
        scraped_at_iso = str(scraped_at) if scraped_at is not None else ""

    updated_label_raw = payload.get("updated_label_raw", "")

    dg = payload.get("detalles_generacion", {}) or {}
    pd = payload.get("pronostico_demanda", {}) or {}
    fx = payload.get("fuel_mix_pct", {}) or {}

    def num(x):
        return float(x) if x is not None else None

    row = [
        scraped_at_iso,
        updated_label_raw,
        num(dg.get("generacion_total_MW")),
        num(dg.get("capacidad_disponible_MW")),
        num(dg.get("reserva_en_rotacion_MW")),
        num(dg.get("reserva_operacional_MW")),
        num(pd.get("proxima_hora_MW")),
        num(pd.get("maxima_hoy_MW")),
        num(fx.get("gas_natural")),
        num(fx.get("carbon")),
        num(fx.get("bunker_diesel")),
        num(fx.get("renovables")),
        num(fx.get("otros")),
        ",".join(payload.get("source_pages", []) or []),
    ]
    return row

def write_csv(csv_path: Path, row: list):
    ensure_csv(csv_path)
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

def write_sqlite(db_path: Path, row: list):
    ensure_sqlite(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        INSERT OR IGNORE INTO readings (
            scraped_at_iso, updated_label_raw,
            generacion_total_MW, capacidad_disponible_MW, reserva_en_rotacion_MW, reserva_operacional_MW,
            demanda_proxima_hora_MW, demanda_maxima_hoy_MW,
            gas_natural_pct, carbon_pct, bunker_diesel_pct, renovables_pct, otros_pct,
            source_pages
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, row)
    conn.commit()
    conn.close()

def process_once(json_path: Path, csv_path: Path, db_path: Path, verbose: bool):
    if not json_path.exists():
        if verbose: print(f"[WARN] JSON not found: {json_path}")
        return

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ERROR] Failed to read/parse JSON: {e}", file=sys.stderr)
        return

    row = parse_row(payload)

    # Dedup via SQLite primary key on scraped_at_iso
    write_sqlite(db_path, row)

    # Also append to CSV (still idempotent enough; duplicates avoided by SQLite)
    write_csv(csv_path, row)

    if verbose:
        print(f"[OK] Ingested scraped_at={row[0]} → CSV:{csv_path.name}, DB:{db_path.name}")

def main():
    ap = argparse.ArgumentParser(description="Ingest Puerto Rico grid snapshot JSON into CSV/SQLite.")
    ap.add_argument("--file", required=True, help="Path to snapshot.json")
    ap.add_argument("--csv", default="generation_log.csv", help="CSV output path")
    ap.add_argument("--db", default="generation_log.sqlite", help="SQLite output path")
    ap.add_argument("--interval", type=int, default=300, help="Seconds between polls (ignored with --once)")
    ap.add_argument("--once", action="store_true", help="Run a single ingestion and exit")
    ap.add_argument("--verbose", action="store_true", help="Print progress")
    args = ap.parse_args()

    json_path = Path(args.file).expanduser().resolve()
    csv_path  = Path(args.csv).expanduser().resolve()
    db_path   = Path(args.db).expanduser().resolve()

    if args.once:
        process_once(json_path, csv_path, db_path, args.verbose)
        return

    # Long-running loop
    last_seen_mtime = None
    if args.verbose:
        print(f"[RUNNING] Watching {json_path} every {args.interval}s")

    try:
        while True:
            # Only ingest if file changed OR always ingest (since data is a fresh scrape)
            try:
                mtime = json_path.stat().st_mtime
            except FileNotFoundError:
                mtime = None

            if mtime is None:
                if args.verbose: print("[WAIT] snapshot.json not found, retrying…")
            else:
                # Ingest every loop regardless, but mtime check helps avoid useless reads if unchanged
                if last_seen_mtime is None or mtime != last_seen_mtime:
                    process_once(json_path, csv_path, db_path, args.verbose)
                    last_seen_mtime = mtime
                else:
                    if args.verbose: print("[SKIP] No change detected")

            time.sleep(max(1, args.interval))
    except KeyboardInterrupt:
        if args.verbose: print("\n[EXIT] Stopped by user.")

if __name__ == "__main__":
    main()
