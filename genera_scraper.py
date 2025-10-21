# genera_scraper.py
# Scrapes Genera PR realtime pages and prints a single JSON snapshot.
# Requirements: pip install playwright; python -m playwright install

import asyncio, json, re, sys, time
from datetime import datetime
from typing import Optional, Dict, Any, List

from playwright.async_api import async_playwright

PAGES = [
    "https://genera-pr.com/data-tr",
    "https://genera-pr.com/data-generacion",
]

# Labels we care about (exact Spanish headings as shown on the site)
DETALLE_LABELS = [
    ("Generación Total del Sistema", "MW", "generacion_total_MW"),
    ("Capacidad Disponible", "MW", "capacidad_disponible_MW"),
    ("Reserva en Rotación", "MW", "reserva_en_rotacion_MW"),
    ("Reserva Operacional", "MW", "reserva_operacional_MW"),
]

PRONOSTICO_LABELS = [
    ("Demanda Próxima Hora", "MW", "proxima_hora_MW"),
    ("Demanda Máxima Registrada Hoy", "MW", "maxima_hoy_MW"),
]

# Common fuel mix labels; may vary—add more if you see them on the site
FUEL_LABELS = [
    ("Gas Natural", "%", "gas_natural"),
    ("Carbón", "%", "carbon"),
    ("Bunker/Diésel", "%", "bunker_diesel"),
    ("Bunker / Diésel", "%", "bunker_diesel"),  # alt spacing
    ("Renovables", "%", "renovables"),
    ("Otros", "%", "otros"),
]

UPDATED_LABEL = "Actualizado"

def _to_float(num_str: Optional[str]) -> Optional[float]:
    if not num_str:
        return None
    # Accept 1,234.56 or 1.234,56 -> normalize to dot decimal
    s = num_str.strip()
    # If both comma and dot exist, assume comma is thousands
    if "," in s and "." in s:
        s = s.replace(",", "")
    else:
        # If only comma exists, treat as decimal separator
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def _search_number_after_label(page_text: str, label: str, unit: str) -> Optional[float]:
    # Try same-line / nearby capture
    # Example: "Generación Total del Sistema\n2129 MW"
    pattern = rf"{re.escape(label)}(?:[^\n\r]*?\n|\s+)*?([\d\.,]+)\s*{re.escape(unit)}"
    m = re.search(pattern, page_text, re.IGNORECASE)
    if m:
        return _to_float(m.group(1))
    return None

def _search_timestamp(page_text: str) -> Optional[str]:
    # Capture anything after "Actualizado" up to EOL
    # Example: "Actualizado 10/20/2025 1:06:02 PM"
    m = re.search(rf"{re.escape(UPDATED_LABEL)}\s*([^\n\r]+)", page_text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None

async def _nearest_value_after_label(page) -> str:
    # Small helper injected into the page to try DOM-near extraction
    return """
    (label, unit) => {
      const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const matchesNum = (t, unit) => {
        if (!t) return null;
        const m = t.match(new RegExp('([\\d.,]+)\\s*' + esc(unit), 'i'));
        return m ? m[1] : null;
      };
      const all = Array.from(document.querySelectorAll('*'));
      const candidates = all.filter(el => el.textContent && el.textContent.toLowerCase().includes(label.toLowerCase()));
      if (!candidates.length) return null;

      const seen = new Set();
      for (const el of candidates) {
        // Walk forward siblings and up one level if needed
        let cur = el;
        for (let steps = 0; steps < 60 && cur; steps++) {
          if (!seen.has(cur)) {
            const v = matchesNum(cur.textContent, unit);
            if (v) return v;
            seen.add(cur);
          }
          const next = cur.nextElementSibling || (cur.parentElement && cur.parentElement.nextElementSibling);
          cur = next || (cur.parentElement && cur.parentElement.nextElementSibling);
        }
      }
      // Fallback: first element anywhere with "<num> unit"
      for (const el of all) {
        const v = matchesNum(el.textContent, unit);
        if (v) return v;
      }
      return null;
    }
    """
# ---------- Fuel mix helpers ----------
FUEL_REGEX = re.compile(
    r"(Gas\s*Natural|Carb[oó]n|Bunker\s*/?\s*Di[ée]sel|Renovables|Otros)\s*[:\-]?\s*([\d\.,]+)\s*%",
    re.IGNORECASE
)

LABEL_NORMALIZE = {
    "gas natural": "gas_natural",
    "carbón": "carbon",
    "carbon": "carbon",
    "bunker diésel": "bunker_diesel",
    "bunker diesel": "bunker_diesel",
    "renovables": "renovables",
    "otros": "otros",
}

async def click_if_exists(page, text: str) -> None:
    """Best-effort click on a tab/button by visible text; ignore failures."""
    try:
        await page.get_by_text(text, exact=False).click(timeout=1200)
        await page.wait_for_timeout(800)
    except Exception:
        pass

def parse_fuel_mix(page_text: str) -> Dict[str, Optional[float]]:
    """Extract fuel mix percentages from text."""
    out = {
        "gas_natural": None,
        "carbon": None,
        "bunker_diesel": None,
        "renovables": None,
        "otros": None,
    }
    for m in FUEL_REGEX.finditer(page_text):
        label = m.group(1).strip().lower()
        label = re.sub(r"\s*/\s*", " ", label)
        label = label.replace("ó", "o").replace("é", "e").strip()
        key = LABEL_NORMALIZE.get(label, None)
        if key:
            out[key] = _to_float(m.group(2))
    return out
# ---------- End fuel mix helpers ----------

async def scrape_once(headless: bool = True, timeout_ms: int = 20000) -> Dict[str, Any]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent="HedgeEngine-Scraper/1.0 (+contact@example.com)")
        page = await context.new_page()

        page_texts: List[str] = []
        for url in PAGES:
            try:
                await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
                # Give client-side JS a moment to populate numbers
                await page.wait_for_timeout(1200)
                page_texts.append(await page.inner_text("body"))
            except Exception:
                # Capture what we can and move on
                try:
                    page_texts.append(await page.inner_text("body"))
                except Exception:
                    page_texts.append("")

        merged_text = "\n".join(page_texts)
        # --- Try to reveal the fuel mix "Porcentajes" view and re-merge text ---
        try:
            await page.goto(PAGES[0], wait_until="networkidle", timeout=timeout_ms)
            await page.wait_for_timeout(1200)
            await click_if_exists(page, "Porcentajes")  # no-op if not present
            await page.wait_for_timeout(1700)           # give charts time to render
            porcentajes_text = await page.inner_text("body")
            merged_text = merged_text + "\n" + porcentajes_text
        except Exception:
            pass

        merged_text = "\n".join(page_texts)



        # Build the JSON skeleton
        out: Dict[str, Any] = {
            "scraped_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "updated_label_raw": _search_timestamp(merged_text),
            "detalles_generacion": {},
            "pronostico_demanda": {},
            "fuel_mix_pct": {},
            "source_pages": PAGES,
        }

        # First pass: regex over full text
        for label, unit, key in DETALLE_LABELS:
            val = _search_number_after_label(merged_text, label, unit)
            out["detalles_generacion"][key] = val

        for label, unit, key in PRONOSTICO_LABELS:
            val = _search_number_after_label(merged_text, label, unit)
            out["pronostico_demanda"][key] = val

        parsed_mix = parse_fuel_mix(merged_text)
        for _, _, key in FUEL_LABELS:
            out["fuel_mix_pct"].setdefault(key, None)
        for k, v in parsed_mix.items():
            if v is not None:
                out["fuel_mix_pct"][k] = v


        # Second pass: DOM-near fallback for any missing values
        missing = []
        for label, unit, key in DETALLE_LABELS + PRONOSTICO_LABELS + FUEL_LABELS:
            bucket = (
                out["detalles_generacion"] if key in [k for _, _, k in DETALLE_LABELS]
                else out["pronostico_demanda"] if key in [k for _, _, k in PRONOSTICO_LABELS]
                else out["fuel_mix_pct"]
            )
            if bucket.get(key) is None:
                missing.append((label, unit, key))

        if missing:
            # Revisit first page for fallback attempts
            try:
                await page.goto(PAGES[0], wait_until="networkidle", timeout=timeout_ms)
                await page.wait_for_timeout(1200)
                helper = await page.evaluate_handle(await _nearest_value_after_label(page))
                for label, unit, key in missing:
                    try:
                        val_str = await page.evaluate(helper, label, unit)
                        val = _to_float(val_str)
                    except Exception:
                        val = None

                    bucket = (
                        out["detalles_generacion"] if key in [k for _, _, k in DETALLE_LABELS]
                        else out["pronostico_demanda"] if key in [k for _, _, k in PRONOSTICO_LABELS]
                        else out["fuel_mix_pct"]
                    )
                    if bucket.get(key) is None and val is not None:
                        bucket[key] = val
            except Exception:
                pass

        # Clean empty groups -> keep keys but allow nulls for transparency
        await browser.close()
        return out

if __name__ == "__main__":
    # CLI: python genera_scraper.py > snapshot.json
    headless = True
    if "--headed" in sys.argv:
        headless = False

    snapshot = asyncio.run(scrape_once(headless=headless))
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
