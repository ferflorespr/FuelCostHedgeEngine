# genera_plants_scraper.py
# Scrapes plant-level generation for Genera PR (Data Generación page).
# Requires: pip install playwright ; python3 -m playwright install

from lib2to3.fixes.fix_input import context
import asyncio, json, re, sys, argparse
from typing import Dict, Any, List, Optional
from datetime import datetime
from playwright.async_api import async_playwright, Page

TARGET_URL = "https://genera-pr.com/data-generacion"

# Known plant/group labels we care about (add variants if site wording changes)
PLANT_LABELS = [
    "San Juan",
    "Palo Seco",
    "Aguirre",
    "Costa Sur",
]
PRIVATE_LABELS = [
    "Ecoeléctrica",
    "AES",
]

MW_RE = re.compile(r"([\d\.,]+)\s*MW\b", re.IGNORECASE)

def _to_float(s: str) -> Optional[float]:
    if not s: return None
    x = s.strip()
    if "," in x and "." in x:
        x = x.replace(",", "")
    elif "," in x:
        x = x.replace(".", "").replace(",", ".")
    try:
        return float(x)
    except:
        return None

def _extract_mw_values(text: str) -> List[float]:
    vals = []
    for m in MW_RE.finditer(text or ""):
        f = _to_float(m.group(1))
        if f is not None:
            vals.append(f)
    return vals

async def _click_if_exists(page: Page, txt: str, timeout_ms: int = 1200):
    try:
        await page.get_by_text(txt, exact=False).first.click(timeout=timeout_ms)
        await page.wait_for_timeout(300)
    except:
        pass

async def _closest_card_with_title(page: Page, title: str):
    """
    Returns a locator for the card/container that includes the given title text.
    """
    # Try heading match first
    loc = page.locator(f"xpath=//*[self::h1 or self::h2 or self::h3 or self::h4][contains(., '{title}')]")
    count = await loc.count()
    if count == 0:
        # fallback: any element containing the title
        loc = page.get_by_text(title, exact=False)
        count = await loc.count()
        if count == 0:
            return None
    el = loc.first
    # Climb to a likely card container (section/article/div with role/aria/class)
    card = el.locator("xpath=ancestor::*[self::section or self::article or self::div][1]")
    return card

async def _open_details_for_card(card, page: Page):
    """
    Try the obvious actions to reveal details: click 'Ver Data' / 'Ver Datos' or similar,
    or click the card itself if it’s expandable.
    """
    # Buttons/links near the card
    try:
        btn = card.get_by_text("Ver Data", exact=False)
        if await btn.count() > 0:
            await btn.first.click()
            await page.wait_for_timeout(400)
            return True
    except: pass
    try:
        btn = card.get_by_text("Ver Datos", exact=False)
        if await btn.count() > 0:
            await btn.first.click()
            await page.wait_for_timeout(400)
            return True
    except: pass
    # Sometimes the card itself toggles details
    try:
        await card.first.click()
        await page.wait_for_timeout(300)
        return True
    except: pass
    return False

async def _read_visible_modal_or_panel(page: Page) -> str:
    """
    Returns the text content from the most likely modal/panel that appears after 'Ver Data'.
    Searches for role=dialog, [aria-modal], or common modal classes.
    """
    # Try ARIA dialog
    for sel in [
        "[role='dialog']",
        "[aria-modal='true']",
        ".modal, .MuiDialog-container, .ant-modal, .dialog, .chakra-modal__content-container",
    ]:
        loc = page.locator(sel)
        if await loc.count() > 0:
            try:
                txt = await loc.first.inner_text()
                if txt and len(txt.strip()) > 0:
                    return txt
            except:
                continue
    # Fallback: grab text from the whole page region near bottom (not ideal but works)
    try:
        return await page.inner_text("body")
    except:
        return ""

async def _close_modal_if_any(page: Page):
    # Try common close patterns so we don’t stack dialogs
    for label in ["Cerrar", "Close", "×", "Ok", "Aceptar"]:
        try:
            await page.get_by_text(label, exact=False).first.click(timeout=400)
            await page.wait_for_timeout(200)
            return
        except:
            continue
    # Try [role=dialog] and hit Escape as a last resort
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(200)
    except:
        pass
import asyncio
from playwright.async_api import TimeoutError as PWTimeout

async def robust_goto(page, url: str, wait_selector: str, nav_timeout_ms: int = 45000):
    """Go to url without relying on 'networkidle'. Wait for DOM + a stable selector, with retries."""
    # cut noisy resources to speed up + reduce hangs
    try:
        await page.route("**/*", lambda route: (
            route.abort()
            if any(u in route.request.url for u in ["googletagmanager", "gtm.js", "analytics", "facebook", "hotjar", "doubleclick"])
            else route.continue_()
        ))
    except Exception:
        pass

    # one retry with backoff
    for attempt in (1, 2):
        try:
            # don't use networkidle; it may never settle
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            # give JS a moment to kick in
            await page.wait_for_timeout(800)
            # now wait for a concrete element/text that proves content rendered
            await page.wait_for_selector(wait_selector, timeout=nav_timeout_ms)
            return True
        except PWTimeout:
            if attempt == 2:
                raise
            await page.wait_for_timeout(1500)  # backoff and retry
    return False

async def scrape_plants(headless: bool = True, timeout_ms: int = 20000) -> Dict[str, Any]:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent="HedgeEngine-Scraper/1.0 (+contact@example.com)")
        context.set_default_timeout(max(timeout_ms, 45000))
        context.set_default_navigation_timeout(max(timeout_ms, 45000))

        page = await context.new_page()

        # a selector that exists on the Data Generación page.
        # try several, the first that matches will unblock the wait.
        selectors = [
            "text=Data Generación",
            "text=Generación",
            "text=Ver Data",
            "h2:has-text('San Juan')",
            "h2:has-text('Palo Seco')",
            "h2:has-text('Aguirre')",
            "h2:has-text('Costa Sur')",
        ]
        loaded = False
        for sel in selectors:
            try:
                await robust_goto(page, TARGET_URL, sel, nav_timeout_ms=max(timeout_ms, 45000))
                loaded = True
                break
            except PWTimeout:
                continue

        if not loaded:
            # last resort: go with 'load' and keep going
            await page.goto(TARGET_URL, wait_until="load", timeout=max(timeout_ms, 45000))
            await page.wait_for_timeout(1500)


        # Try to reveal any sections
        await _click_if_exists(page, "Plantas")
        await _click_if_exists(page, "Generación")
        await _click_if_exists(page, "Ver Data")
        # Give time for any modal/panel to render
        try:
            await page.wait_for_selector("[role='dialog'], [aria-modal='true'], .modal, .ant-modal, .MuiDialog-container", timeout=3000)
        except PWTimeout:
            pass
        # Timestamp if present
        updated_label_raw = None
        try:
            body_text = await page.inner_text("body")
            # capture 'Actualizado ...'
            m = re.search(r"Actualizado\s+([^\n\r]+)", body_text, re.IGNORECASE)
            if m:
                updated_label_raw = m.group(1).strip()
        except:
            pass

        out: Dict[str, Any] = {
            "scraped_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "updated_label_raw": updated_label_raw,
            "plants": [],
            "private_producers": [],
            "source_page": TARGET_URL,
        }

        async def harvest_for(title: str) -> Optional[Dict[str, Any]]:
            card = await _closest_card_with_title(page, title)
            if card is None:
                return None
            # Attempt to open details
            await _open_details_for_card(card, page)
            await page.wait_for_timeout(600)
            detail_text = await _read_visible_modal_or_panel(page)

            # Extract all MW numbers visible
            unit_values = _extract_mw_values(detail_text)
            total_mw = sum(unit_values) if unit_values else None

            # Try to split per-unit lines: look for "Unidad|Unit|U|TG|SJ|PS|AG|CS" markers
            per_unit: List[Dict[str, Any]] = []
            lines = [ln for ln in detail_text.splitlines() if ln.strip()]
            for ln in lines:
                # pick a single MW per line (first match)
                m = MW_RE.search(ln)
                if not m:
                    continue
                val = _to_float(m.group(1))
                # Unit name guess = text before the match (trim heavy whitespace)
                name_guess = ln[:m.start()].strip().strip(":").strip("-").strip()
                if not name_guess:
                    # fallback to the title with an index
                    name_guess = f"{title} unidad"
                per_unit.append({"unidad": name_guess, "mw": val})

            # Close dialog/panel to avoid stacking
            await _close_modal_if_any(page)

            return {
                "nombre": title,
                "total_MW": total_mw,
                "unidades": per_unit
            }

        # Plants
        for name in PLANT_LABELS:
            try:
                item = await harvest_for(name)
                if item:
                    out["plants"].append(item)
            except:
                continue

        # Private producers
        for name in PRIVATE_LABELS:
            try:
                item = await harvest_for(name)
                if item:
                    out["private_producers"].append(item)
            except:
                continue

        await browser.close()
        return out

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--headed", action="store_true", help="Show the browser while scraping")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    data = asyncio.run(scrape_plants(headless=not args.headed))
    print(json.dumps(data, ensure_ascii=False, indent=2))
