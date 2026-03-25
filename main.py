import asyncio
import csv
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional

from playwright.async_api import async_playwright
import pandas as pd
import math

# CONFIG
TARGET_URLS = [
    # Replace with public product or listing pages you own or have permission to test.
    "https://example.com/product/1",
    # add more...
]
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)
CSV_PATH = OUTPUT_DIR / "results.csv"

DISCLOSURE_TEXT = "THIS PRICE WAS SET BY AN ALGORITHM USING YOUR PERSONAL DATA"
VIEWPORT = {"width": 1280, "height": 800}
NAV_TIMEOUT = 30000  # ms

# Personas: adjust user_agent, geolocation (latitude/longitude), and locale/zip if needed.
PERSONAS = [
    {"name": "persona_a", "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117", "locale": "en-US", "geolocation": {"latitude": 40.7128, "longitude": -74.0060}, "timezone": "America/New_York", "zip": "10001"},
    {"name": "persona_b", "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) Safari/605.1.15", "locale": "en-US", "geolocation": {"latitude": 40.730610, "longitude": -73.935242}, "timezone": "America/New_York", "zip": "11201"},
    {"name": "persona_c", "user_agent": "Mozilla/5.0 (Linux; Android 11) Chrome/116", "locale": "en-US", "geolocation": {"latitude": 40.6782, "longitude": -73.9442}, "timezone": "America/New_York", "zip": "10453"},
]

# Helpers
def save_screenshot_path(url_slug: str, persona: str) -> Path:
    p = OUTPUT_DIR / f"{url_slug}__{persona}.png"
    return p

def slugify(url: str) -> str:
    return url.replace("https://", "").replace("http://", "").replace("/", "_").replace("?", "_")

async def evaluate_prominence(page) -> Dict[str, Any]:
    # Evaluate whether disclosure text exists and compute basic prominence metrics:
    # - visible: boundingClientRect present & intersects viewport
    # - font size
    # - contrast estimate by sampling computed color and background-color (approx)
    js = f"""
    (() => {{
      const text = {json.dumps(DISCLOSURE_TEXT)};
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
      const nodes = [];
      while(walker.nextNode()) {{
        if(walker.currentNode.nodeValue && walker.currentNode.nodeValue.trim().toUpperCase().includes(text)) {{
          nodes.push(walker.currentNode.parentElement);
        }}
      }}
      if(nodes.length === 0) return {{found: false}};
      const el = nodes[0];
      const rect = el.getBoundingClientRect();
      const style = window.getComputedStyle(el);
      // compute center visibility
      const viewport = {{w: window.innerWidth, h: window.innerHeight}};
      const visible = rect.width>0 && rect.height>0 && rect.top < viewport.h && rect.bottom > 0;
      const fontSize = parseFloat(style.fontSize) || null;
      const color = style.color || null;
      const bg = style.backgroundColor || null;
      const position = (rect.top/viewport.h);
      return {{
        found: true,
        text_preview: el.innerText.slice(0,200),
        bounding: {{top: rect.top, left: rect.left, width: rect.width, height: rect.height}},
        visible: visible,
        font_size_px: fontSize,
        color: color,
        background_color: bg,
        position_ratio: position
      }};
    }})();
    """
    return await page.evaluate(js)

async def extract_prices(page) -> List[str]:
    # heuristic price extraction: find elements with $ and digits
    js = """
    (() => {
      const results = [];
      const candidates = Array.from(document.querySelectorAll('body *'));
      for(const el of candidates){
        try{
          const txt = el.innerText || '';
          if(/\\$\\s*\\d/.test(txt)){
            results.push({text: txt.trim().slice(0,200), xpath: 'N/A'});
          }
        }catch(e){}
      }
      // dedupe
      const uniq = [];
      const seen = new Set();
      for(const r of results){ if(!seen.has(r.text)){ seen.add(r.text); uniq.push(r); } }
      return uniq.slice(0,20);
    })();
    """
    return await page.evaluate(js)

async def run_for_persona(playwright, url, persona):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent=persona["user_agent"],
        viewport=VIEWPORT,
        locale=persona.get("locale", "en-US"),
        geolocation=persona.get("geolocation"),
        timezone_id=persona.get("timezone"),
        permissions=["geolocation"],
        record_har_path=None,
    )
    page = await context.new_page()
    results = {"url": url, "persona": persona["name"], "disclosure_found": False, "prominence": None, "prices": [], "error": None}
    try:
        await page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT)
        # Optional: simulate simple browsing history to encourage personalization
        # e.g., navigate to category pages before product:
        await asyncio.sleep(1)
        # take snapshot
        slug = slugify(url)
        screenshot_path = save_screenshot_path(slug, persona["name"])
        await page.screenshot(path=str(screenshot_path), full_page=True)
        # extract prices and disclosure prominence
        prices = await extract_prices(page)
        results["prices"] = prices
        prominence = await evaluate_prominence(page)
        results["disclosure_found"] = prominence.get("found", False)
        results["prominence"] = prominence
        # save page html
        html_path = OUTPUT_DIR / f"{slug}__{persona['name']}.html"
        open(html_path, "w", encoding="utf-8").write(await page.content())
    except Exception as e:
        results["error"] = str(e)
    finally:
        await context.close()
        await browser.close()
    return results

async def main():
    # ensure CSV header
    fieldnames = ["url", "persona", "disclosure_found", "prominence", "prices", "error"]
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    async with async_playwright() as p:
        for url in TARGET_URLS:
            for persona in PERSONAS:
                # Respectful delay between requests
                await asyncio.sleep(3)
                res = await run_for_persona(p, url, persona)
                # flatten and append to CSV
                row = {
                    "url": res["url"],
                    "persona": res["persona"],
                    "disclosure_found": res["disclosure_found"],
                    "prominence": json.dumps(res["prominence"]),
                    "prices": json.dumps(res["prices"]),
                    "error": res["error"],
                }
                with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(row)
                print(f"Done: {url} / {persona['name']} -> disclosure:{res['disclosure_found']}")

if __name__ == "__main__":
    asyncio.run(main())
