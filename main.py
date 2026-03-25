"""
NY Algorithmic Pricing Disclosure Act — compliance visibility audit (research prototype).

Audits whether required disclosure text appears and quantifies accessibility metrics
(font size, contrast, placement, scroll, estimated click friction). This tool measures
disclosure visibility and prominence; it does not establish that personalized pricing occurred.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from playwright.async_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Page,
    async_playwright,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_DISCLOSURE = (
    "THIS PRICE WAS SET BY AN ALGORITHM USING YOUR PERSONAL DATA"
)

DEFAULT_VIEWPORT = {"width": 1280, "height": 800}
DEFAULT_NAV_TIMEOUT_MS = 45_000
DEFAULT_MAX_CONCURRENCY = 2
NEAR_PRICE_VERTICAL_PX = 120

# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GeolocationConfig:
    """WGS84 coordinates for Playwright context geolocation."""

    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not (-90 <= self.latitude <= 90):
            raise ValueError("latitude must be in [-90, 90]")
        if not (-180 <= self.longitude <= 180):
            raise ValueError("longitude must be in [-180, 180]")


@dataclass(frozen=True)
class PersonaConfig:
    """Isolated browser context settings for a synthetic visitor profile."""

    name: str
    user_agent: str
    locale: str = "en-US"
    timezone: str = "America/New_York"
    geolocation: GeolocationConfig | None = None
    storage_state_path: Path | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("persona name must be non-empty")


@dataclass
class AuditConfig:
    """Top-level run configuration (CLI + defaults)."""

    urls: list[str]
    output_dir: Path
    headless: bool = True
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    capture_screenshot: bool = True
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS
    viewport: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_VIEWPORT))
    personas: list[PersonaConfig] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Result models (research-oriented schema)
# ---------------------------------------------------------------------------

PositionRelativeToPrice = Literal["above_price", "near_price", "below_price", "unknown", "separate_page"]


@dataclass
class BoundingBox:
    top: float
    left: float
    width: float
    height: float


@dataclass
class DisclosureCandidate:
    """One DOM match for the disclosure with measured accessibility attributes."""

    matched_text: str
    dom_selector_hint: str
    tag_name: str
    found_exact: bool
    found_normalized: bool
    font_size_px: float | None
    font_weight: str | None
    text_color_css: str | None
    background_color_css: str | None
    contrast_ratio: float | None
    bounding_box: BoundingBox | None
    visible_in_viewport: bool
    distance_from_document_top_px: float | None
    requires_scroll: bool
    clicks_required: int
    clicks_estimate_is_approximate: bool
    position_relative_to_price: PositionRelativeToPrice
    number_of_matches_on_page: int


@dataclass
class DisclosureEvaluation:
    """Aggregated disclosure search: exact/normalized flags, best pick, all candidates."""

    found_exact: bool
    found_normalized: bool
    matched_text: str | None
    number_of_matches: int
    best_candidate: DisclosureCandidate | None
    all_candidates: list[DisclosureCandidate]


@dataclass
class PriceCandidate:
    value: float | None
    currency_hint: str | None
    raw_text: str
    dom_context: str


@dataclass
class PageRunResult:
    """Single URL × persona audit record (flattened fields for CSV/JSON)."""

    run_id: str
    timestamp_iso: str
    url: str
    persona_name: str
    disclosure_found_exact: bool
    disclosure_found_normalized: bool
    disclosure_visible: bool
    disclosure_font_size_px: float | None
    disclosure_contrast_ratio: float | None
    disclosure_clicks_required: int
    disclosure_position_relative_to_price: PositionRelativeToPrice
    disclosure_distance_from_top: float | None
    disclosure_requires_scroll: bool
    disclosure_match_count: int
    top_price_text: str | None
    num_price_candidates: int
    screenshot_path: str | None
    html_path: str | None
    json_path: str | None
    error: str | None
    # Full nested payloads for JSON export
    disclosure: DisclosureEvaluation | None = None
    prices: list[PriceCandidate] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Text normalization & contrast (Python)
# ---------------------------------------------------------------------------


def normalize_disclosure_text(s: str) -> str:
    """Case-insensitive, whitespace-tolerant normalization for matching."""
    return " ".join(s.split()).upper()


# ---------------------------------------------------------------------------
# Safe I/O
# ---------------------------------------------------------------------------


def ensure_run_dir(base: Path, run_id: str) -> Path:
    d = (base / run_id).resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d


def slugify_url(url: str) -> str:
    s = re.sub(r"^https?://", "", url, flags=re.I)
    s = re.sub(r"[^\w.\-]+", "_", s)
    return s[:180] if len(s) > 180 else s


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Browser: navigation & light dismissal
# ---------------------------------------------------------------------------


COOKIE_SELECTORS = [
    'button:has-text("Accept")',
    'button:has-text("I Agree")',
    'button:has-text("OK")',
    '[aria-label*="accept" i]',
    '[id*="cookie" i] button',
    ".osano-cm-accept",
    "#onetrust-accept-btn-handler",
]


async def robust_goto(page: Page, url: str, timeout_ms: int, log: logging.Logger) -> None:
    """Try networkidle → load → domcontentloaded; raise last error if all fail."""
    strategies: list[tuple[str, str]] = [
        ("networkidle", "networkidle"),
        ("load", "load"),
        ("domcontentloaded", "domcontentloaded"),
    ]
    last_err: Exception | None = None
    for label, wait_until in strategies:
        try:
            await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            log.debug("Loaded %s with wait_until=%s", url, label)
            return
        except PlaywrightError as e:
            last_err = e
            log.warning("goto %s failed (%s): %s", url, label, e)
    if last_err:
        raise last_err


async def dismiss_common_overlays(page: Page, log: logging.Logger) -> None:
    for sel in COOKIE_SELECTORS:
        try:
            loc = page.locator(sel).first
            if await loc.count() == 0:
                continue
            if await loc.is_visible(timeout=800):
                await loc.click(timeout=2000)
                log.debug("Clicked overlay selector: %s", sel)
                await asyncio.sleep(0.3)
        except PlaywrightError:
            continue


# ---------------------------------------------------------------------------
# In-page evaluation (disclosure + prices) — single evaluate script
# ---------------------------------------------------------------------------

EVAL_DISCLOSURE_AND_PRICES = """
(args) => {
  const TARGET = args.target;
  const NEAR_PX = args.nearPricePx;
  const inIframe = !!args.inIframe;

  function normalizeText(s) {
    return (s || "").replace(/\\s+/g, " ").trim().toUpperCase();
  }
  const TARGET_NORM = normalizeText(TARGET);

  function parseRgb(color) {
    if (!color || color === "transparent") return null;
    const m = color.match(/rgba?\\(\\s*([\\d.]+)\\s*,\\s*([\\d.]+)\\s*,\\s*([\\d.]+)/i);
    if (!m) return null;
    return { r: +m[1], g: +m[2], b: +m[3] };
  }

  function luminance(rgb) {
    function lin(c) {
      c = c / 255;
      return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    }
    const r = lin(rgb.r), g = lin(rgb.g), b = lin(rgb.b);
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  }

  function contrastRatio(fg, bg) {
    const L1 = luminance(fg) + 0.05;
    const L2 = luminance(bg) + 0.05;
    const hi = Math.max(L1, L2), lo = Math.min(L1, L2);
    return hi / lo;
  }

  function effectiveBackground(el) {
    let node = el;
    for (let i = 0; i < 12 && node; i++) {
      const bg = window.getComputedStyle(node).backgroundColor;
      const p = parseRgb(bg);
      if (p && bg !== "transparent") {
        const m = bg.match(/rgba?\\([^)]+\\)/);
        if (m && bg.includes("rgba")) {
          const a = parseFloat(bg.split(",").pop());
          if (!isNaN(a) && a < 0.05) { node = node.parentElement; continue; }
        }
        return p;
      }
      node = node.parentElement;
    }
    return { r: 255, g: 255, b: 255 };
  }

  function estimateClicks(el) {
    let clicks = 0;
    let approx = false;
    let node = el;
    while (node && node !== document.documentElement) {
      const st = window.getComputedStyle(node);
      const tag = (node.tagName || "").toLowerCase();
      if (st.display === "none" || st.visibility === "hidden" || parseFloat(st.opacity || "1") === 0) {
        clicks += 1;
        approx = true;
      }
      if (tag === "details" && !node.open) { clicks += 1; approx = true; }
      if (tag === "dialog" && !node.open) { clicks += 1; approx = true; }
      if (node.getAttribute && node.getAttribute("aria-hidden") === "true") { clicks += 1; approx = true; }
      if (node.getAttribute && node.getAttribute("hidden") !== null) { clicks += 1; approx = true; }
      node = node.parentElement;
    }
    return { clicks: Math.min(clicks, 8), approx };
  }

  function selectorHint(el) {
    if (!el || !el.tagName) return "";
    const tag = el.tagName.toLowerCase();
    if (el.id) return tag + "#" + el.id;
    const cls = (el.className && String(el.className).split(/\\s+/).filter(Boolean)[0]) || "";
    if (cls) return tag + "." + cls;
    return tag;
  }

  const all = Array.from(document.querySelectorAll("body *"));
  const matching = [];
  for (const el of all) {
    let txt;
    try { txt = el.innerText || ""; } catch (e) { continue; }
    const tn = normalizeText(txt);
    if (!tn.includes(TARGET_NORM)) continue;
    const innermost = !all.some(
      (o) => o !== el && el.contains(o) && normalizeText(o.innerText || "").includes(TARGET_NORM)
    );
    if (innermost) matching.push(el);
  }

  let pricePrimary = null;
  const priceRegex = /\\$\\s*\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})?|\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})?\\s*(?:USD|US\\s*\\$|\\$)/gi;
  const priceHits = [];
  const bodyText = document.body ? (document.body.innerText || "") : "";
  let m;
  while ((m = priceRegex.exec(bodyText)) !== null) {
    priceHits.push({ raw: m[0], index: m.index });
  }

  if (priceHits.length) {
    const first = priceHits[0];
    const els = Array.from(document.querySelectorAll("body *")).reverse();
    for (const el of els) {
      try {
        if (!(el.innerText || "").includes(first.raw.trim())) continue;
        const r = el.getBoundingClientRect();
        if (r.width < 2 || r.height < 2) continue;
        pricePrimary = { top: r.top + window.scrollY, left: r.left, width: r.width, height: r.height, text: first.raw };
        break;
      } catch (e) {}
    }
  }

  function classifyPosition(dTop, dCenterY, priceY) {
    if (priceY == null) return "unknown";
    const dy = dCenterY - priceY;
    if (dy < -NEAR_PX) return "above_price";
    if (dy > NEAR_PX) return "below_price";
    if (Math.abs(dy) <= NEAR_PX) return "near_price";
    return "unknown";
  }

  const candidates = [];

  for (const el of matching) {
    const fullText = el.innerText || "";
    const foundExact = fullText.includes(TARGET);
    const foundNorm = normalizeText(fullText).includes(TARGET_NORM);
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    const fs = parseFloat(style.fontSize) || null;
    const fw = style.fontWeight || null;
    const fg = parseRgb(style.color) || { r: 0, g: 0, b: 0 };
    const bg = effectiveBackground(el);
    const cr = contrastRatio(fg, bg);
    const vh = window.innerHeight, vw = window.innerWidth;
    const visible =
      rect.width > 0 &&
      rect.height > 0 &&
      rect.bottom > 0 &&
      rect.right > 0 &&
      rect.top < vh &&
      rect.left < vw;
    const docTop = rect.top + window.scrollY;
    const fullyInViewport =
      rect.top >= 0 && rect.left >= 0 && rect.bottom <= vh && rect.right <= vw;
    const requiresScroll = !fullyInViewport;
    const centerY = docTop + rect.height / 2;
    let priceY = pricePrimary ? pricePrimary.top + (pricePrimary.height || 0) / 2 : null;
    let pos = inIframe ? "separate_page" : classifyPosition(docTop, centerY, priceY);

    const est = estimateClicks(el);
    let clicks = est.clicks;
    let approx = est.approx;
    if (visible && clicks === 0) { /* keep 0 */ }
    else if (!visible && clicks === 0) { clicks = 1; approx = true; }

    const hint = inIframe ? ("iframe::" + selectorHint(el)) : selectorHint(el);
    candidates.push({
      matched_text: fullText.slice(0, 400),
      dom_selector_hint: hint,
      tag_name: el.tagName || "",
      found_exact: foundExact,
      found_normalized: foundNorm,
      font_size_px: fs,
      font_weight: fw,
      text_color_css: style.color || null,
      background_color_css: style.backgroundColor || null,
      contrast_ratio: cr,
      bounding_box: { top: rect.top, left: rect.left, width: rect.width, height: rect.height },
      visible_in_viewport: visible,
      distance_from_document_top_px: docTop,
      requires_scroll: requiresScroll,
      clicks_required: clicks,
      clicks_estimate_is_approximate: approx,
      position_relative_to_price: pos,
      number_of_matches_on_page: matching.length,
    });
  }

  const currencyRegex = /\\$\\s*\\d[\\d,]*(?:\\.\\d{2})?|\\d[\\d,]*(?:\\.\\d{2})?\\s*(?:USD|US\\s*\\$)/gi;
  const priceCandidates = [];
  const seen = new Set();
  const priceEls = Array.from(document.querySelectorAll("body *")).filter((e) => {
    try {
      const t = e.innerText || "";
      return t && currencyRegex.test(t) && (t.length < 400);
    } catch (e) { return false; }
  }).slice(0, 400);

  for (const el of priceEls) {
    const raw = (el.innerText || "").trim().slice(0, 200);
    const mm = raw.match(currencyRegex);
    if (!mm || !mm[0]) continue;
    const key = mm[0] + "|" + selectorHint(el);
    if (seen.has(key)) continue;
    seen.add(key);
    const num = parseFloat(mm[0].replace(/[^\\d.]/g, ""));
    priceCandidates.push({
      value: isNaN(num) ? null : num,
      currency_hint: mm[0].includes("$") ? "USD" : null,
      raw_text: mm[0],
      dom_context: selectorHint(el) + " | " + raw.slice(0, 120),
    });
    if (priceCandidates.length >= 40) break;
  }

  return { candidates, matchCount: matching.length, priceCandidates };
}
"""


def _merge_frame_evaluations(chunks: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge per-frame evaluate() payloads; dedupe price rows later in Python."""
    cands: list[Any] = []
    prices_raw: list[Any] = []
    for ch in chunks:
        cands.extend(ch.get("candidates") or [])
        prices_raw.extend(ch.get("priceCandidates") or [])
    return {"candidates": cands, "matchCount": len(cands), "priceCandidates": prices_raw}


async def evaluate_disclosure_all_frames(page: Page, log: logging.Logger) -> dict[str, Any]:
    """Run disclosure + price heuristics in every frame (main + iframes)."""
    chunks: list[dict[str, Any]] = []
    for fr in page.frames:
        try:
            payload = await fr.evaluate(
                EVAL_DISCLOSURE_AND_PRICES,
                {
                    "target": REQUIRED_DISCLOSURE,
                    "nearPricePx": NEAR_PRICE_VERTICAL_PX,
                    "inIframe": fr != page.main_frame,
                },
            )
            chunks.append(payload)
        except Exception as e:
            log.debug("Frame evaluation skipped: %s", e)
            continue
    return _merge_frame_evaluations(chunks)


def _parse_disclosure_js(raw: dict[str, Any]) -> DisclosureEvaluation:
    cands_js = raw.get("candidates") or []
    n = int(raw.get("matchCount") or 0)
    parsed: list[DisclosureCandidate] = []
    for c in cands_js:
        bb = c.get("bounding_box") or {}
        bbox = BoundingBox(
            top=float(bb.get("top", 0)),
            left=float(bb.get("left", 0)),
            width=float(bb.get("width", 0)),
            height=float(bb.get("height", 0)),
        )
        parsed.append(
            DisclosureCandidate(
                matched_text=c.get("matched_text") or "",
                dom_selector_hint=c.get("dom_selector_hint") or "",
                tag_name=c.get("tag_name") or "",
                found_exact=bool(c.get("found_exact")),
                found_normalized=bool(c.get("found_normalized")),
                font_size_px=c.get("font_size_px"),
                font_weight=c.get("font_weight"),
                text_color_css=c.get("text_color_css"),
                background_color_css=c.get("background_color_css"),
                contrast_ratio=c.get("contrast_ratio"),
                bounding_box=bbox,
                visible_in_viewport=bool(c.get("visible_in_viewport")),
                distance_from_document_top_px=c.get("distance_from_document_top_px"),
                requires_scroll=bool(c.get("requires_scroll")),
                clicks_required=int(c.get("clicks_required") or 0),
                clicks_estimate_is_approximate=bool(c.get("clicks_estimate_is_approximate")),
                position_relative_to_price=c.get("position_relative_to_price") or "unknown",
                number_of_matches_on_page=int(c.get("number_of_matches_on_page") or n),
            )
        )

    found_exact = any(p.found_exact for p in parsed)
    found_norm = any(p.found_normalized for p in parsed)
    best = _pick_best_disclosure(parsed)
    matched = best.matched_text[:200] if best else None
    return DisclosureEvaluation(
        found_exact=found_exact,
        found_normalized=found_norm,
        matched_text=matched,
        number_of_matches=n,
        best_candidate=best,
        all_candidates=parsed,
    )


def _pick_best_disclosure(cands: list[DisclosureCandidate]) -> DisclosureCandidate | None:
    if not cands:
        return None

    def score(c: DisclosureCandidate) -> tuple:
        vis = 1 if c.visible_in_viewport else 0
        dist = c.distance_from_document_top_px if c.distance_from_document_top_px is not None else 1e9
        cr = c.contrast_ratio or 0
        clicks_pen = -min(c.clicks_required, 5)
        return (vis, cr, -dist, clicks_pen)

    return max(cands, key=score)


def _parse_prices(raw: list[dict[str, Any]]) -> list[PriceCandidate]:
    out: list[PriceCandidate] = []
    seen: set[str] = set()
    for p in raw:
        key = (p.get("raw_text") or "") + "|" + (p.get("dom_context") or "")
        if key in seen:
            continue
        seen.add(key)
        out.append(
            PriceCandidate(
                value=p.get("value"),
                currency_hint=p.get("currency_hint"),
                raw_text=p.get("raw_text") or "",
                dom_context=p.get("dom_context") or "",
            )
        )
    return out


# ---------------------------------------------------------------------------
# Core audit step
# ---------------------------------------------------------------------------


async def audit_page(
    page: Page,
    url: str,
    persona: PersonaConfig,
    run_id: str,
    cfg: AuditConfig,
    log: logging.Logger,
) -> PageRunResult:
    ts = datetime.now(timezone.utc).isoformat()
    err: str | None = None
    disc: DisclosureEvaluation | None = None
    prices: list[PriceCandidate] = []
    screenshot_path: str | None = None
    html_path: str | None = None
    json_path: str | None = None

    slug = slugify_url(url)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", persona.name)
    base_name = f"{slug}__{safe}"
    run_dir = ensure_run_dir(cfg.output_dir, run_id)

    try:
        await robust_goto(page, url, cfg.nav_timeout_ms, log)
        await asyncio.sleep(0.4)
        await dismiss_common_overlays(page, log)

        data = await evaluate_disclosure_all_frames(page, log)
        disc = _parse_disclosure_js(data)
        prices = _parse_prices(data.get("priceCandidates") or [])

        html_file = run_dir / f"{base_name}.html"
        write_text_atomic(html_file, await page.content())
        html_path = str(html_file)

        if cfg.capture_screenshot:
            png = run_dir / f"{base_name}.png"
            await page.screenshot(path=str(png), full_page=True)
            screenshot_path = str(png)

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        log.exception("Audit failed for %s (%s)", url, persona.name)

    # Flatten for CSV schema
    bc = disc.best_candidate if disc else None
    top_price = prices[0].raw_text if prices else None

    result = PageRunResult(
        run_id=run_id,
        timestamp_iso=ts,
        url=url,
        persona_name=persona.name,
        disclosure_found_exact=disc.found_exact if disc else False,
        disclosure_found_normalized=disc.found_normalized if disc else False,
        disclosure_visible=bool(bc and bc.visible_in_viewport) if bc else False,
        disclosure_font_size_px=bc.font_size_px if bc else None,
        disclosure_contrast_ratio=bc.contrast_ratio if bc else None,
        disclosure_clicks_required=bc.clicks_required if bc else 0,
        disclosure_position_relative_to_price=(bc.position_relative_to_price if bc else "unknown"),
        disclosure_distance_from_top=bc.distance_from_document_top_px if bc else None,
        disclosure_requires_scroll=bool(bc and bc.requires_scroll) if bc else False,
        disclosure_match_count=disc.number_of_matches if disc else 0,
        top_price_text=top_price,
        num_price_candidates=len(prices),
        screenshot_path=screenshot_path,
        html_path=html_path,
        json_path=json_path,
        error=err,
        disclosure=disc,
        prices=prices,
    )

    # Write per-row JSON
    jf = run_dir / f"{base_name}.json"
    write_text_atomic(jf, json.dumps(_result_to_jsonable(result), indent=2, ensure_ascii=False))
    result.json_path = str(jf)
    return result


def _result_to_jsonable(r: PageRunResult) -> dict[str, Any]:
    def ser_dc(d: DisclosureCandidate) -> dict[str, Any]:
        o = asdict(d)
        o["bounding_box"] = asdict(d.bounding_box) if d.bounding_box else None
        return o

    out: dict[str, Any] = {
        "run_id": r.run_id,
        "timestamp": r.timestamp_iso,
        "url": r.url,
        "persona_name": r.persona_name,
        "disclosure_found_exact": r.disclosure_found_exact,
        "disclosure_found_normalized": r.disclosure_found_normalized,
        "disclosure_visible": r.disclosure_visible,
        "disclosure_font_size_px": r.disclosure_font_size_px,
        "disclosure_contrast_ratio": r.disclosure_contrast_ratio,
        "disclosure_clicks_required": r.disclosure_clicks_required,
        "disclosure_position_relative_to_price": r.disclosure_position_relative_to_price,
        "disclosure_distance_from_top": r.disclosure_distance_from_top,
        "disclosure_requires_scroll": r.disclosure_requires_scroll,
        "disclosure_match_count": r.disclosure_match_count,
        "top_price_text": r.top_price_text,
        "num_price_candidates": r.num_price_candidates,
        "screenshot_path": r.screenshot_path,
        "html_path": r.html_path,
        "json_path": r.json_path,
        "error": r.error,
        "prices": [asdict(p) for p in r.prices],
    }
    if r.disclosure:
        out["disclosure"] = {
            "found_exact": r.disclosure.found_exact,
            "found_normalized": r.disclosure.found_normalized,
            "matched_text": r.disclosure.matched_text,
            "number_of_matches": r.disclosure.number_of_matches,
            "best_candidate": ser_dc(r.disclosure.best_candidate) if r.disclosure.best_candidate else None,
            "all_candidates": [ser_dc(c) for c in r.disclosure.all_candidates],
        }
    return out


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def create_context(
    browser: Browser, persona: PersonaConfig, cfg: AuditConfig
) -> BrowserContext:
    opts: dict[str, Any] = {
        "viewport": cfg.viewport,
        "user_agent": persona.user_agent,
        "locale": persona.locale,
        "timezone_id": persona.timezone,
    }
    if persona.geolocation:
        opts["geolocation"] = {
            "latitude": persona.geolocation.latitude,
            "longitude": persona.geolocation.longitude,
        }
        opts["permissions"] = ["geolocation"]
    if persona.storage_state_path and persona.storage_state_path.is_file():
        opts["storage_state"] = str(persona.storage_state_path)

    return await browser.new_context(**opts)


CSV_FIELDS = [
    "run_id",
    "timestamp",
    "url",
    "persona_name",
    "disclosure_found_exact",
    "disclosure_found_normalized",
    "disclosure_visible",
    "disclosure_font_size_px",
    "disclosure_contrast_ratio",
    "disclosure_requires_scroll",
    "disclosure_clicks_required",
    "disclosure_position_relative_to_price",
    "disclosure_distance_from_top",
    "num_disclosure_matches",
    "top_price_text",
    "num_price_candidates",
    "screenshot_path",
    "html_path",
    "json_path",
    "error",
]


def result_to_csv_row(r: PageRunResult) -> dict[str, Any]:
    return {
        "run_id": r.run_id,
        "timestamp": r.timestamp_iso,
        "url": r.url,
        "persona_name": r.persona_name,
        "disclosure_found_exact": r.disclosure_found_exact,
        "disclosure_found_normalized": r.disclosure_found_normalized,
        "disclosure_visible": r.disclosure_visible,
        "disclosure_font_size_px": r.disclosure_font_size_px,
        "disclosure_contrast_ratio": r.disclosure_contrast_ratio,
        "disclosure_requires_scroll": r.disclosure_requires_scroll,
        "disclosure_clicks_required": r.disclosure_clicks_required,
        "disclosure_position_relative_to_price": r.disclosure_position_relative_to_price,
        "disclosure_distance_from_top": r.disclosure_distance_from_top,
        "num_disclosure_matches": r.disclosure_match_count,
        "top_price_text": r.top_price_text,
        "num_price_candidates": r.num_price_candidates,
        "screenshot_path": r.screenshot_path,
        "html_path": r.html_path,
        "json_path": r.json_path,
        "error": r.error,
    }


async def run_audit(cfg: AuditConfig, log: logging.Logger) -> list[PageRunResult]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    ensure_run_dir(cfg.output_dir, run_id)
    csv_path = cfg.output_dir / run_id / "summary.csv"

    tasks: list[tuple[str, PersonaConfig]] = [(u, p) for u in cfg.urls for p in cfg.personas]
    sem = asyncio.Semaphore(max(1, cfg.max_concurrency))

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=cfg.headless)

        async def one(url: str, persona: PersonaConfig) -> PageRunResult:
            async with sem:
                context = await create_context(browser, persona, cfg)
                page = await context.new_page()
                try:
                    return await audit_page(page, url, persona, run_id, cfg, log)
                finally:
                    await context.close()

        results = await asyncio.gather(*[one(u, p) for u, p in tasks])

        await browser.close()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in results:
            w.writerow(result_to_csv_row(r))

    log.info("Wrote CSV: %s", csv_path)
    return results


def default_personas() -> list[PersonaConfig]:
    return [
        PersonaConfig(
            name="persona_a",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            geolocation=GeolocationConfig(40.7128, -74.0060),
            timezone="America/New_York",
        ),
        PersonaConfig(
            name="persona_b",
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
                "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            locale="en-US",
            geolocation=GeolocationConfig(40.730610, -73.935242),
            timezone="America/New_York",
        ),
        PersonaConfig(
            name="persona_c",
            user_agent=(
                "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
            ),
            locale="en-US",
            geolocation=GeolocationConfig(40.6782, -73.9442),
            timezone="America/New_York",
        ),
    ]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Audit visibility of NY algorithmic pricing disclosure (research prototype)."
    )
    p.add_argument(
        "--urls",
        nargs="+",
        help="One or more URLs to audit (product/listing pages).",
    )
    p.add_argument(
        "--url-file",
        type=Path,
        help="File with one URL per line (ignored if --urls is set).",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Base directory for run artifacts (default: ./output).",
    )
    p.add_argument("--headed", action="store_true", help="Run headed (not headless).")
    p.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help=f"Max parallel browser contexts (default: {DEFAULT_MAX_CONCURRENCY}).",
    )
    p.add_argument(
        "--no-screenshot",
        action="store_true",
        help="Skip PNG screenshots (HTML + JSON still saved).",
    )
    p.add_argument(
        "--nav-timeout-ms",
        type=int,
        default=DEFAULT_NAV_TIMEOUT_MS,
        help=f"Navigation timeout in ms (default: {DEFAULT_NAV_TIMEOUT_MS}).",
    )
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def load_urls(ns: argparse.Namespace) -> list[str]:
    if ns.urls:
        return list(ns.urls)
    if ns.url_file:
        lines = ns.url_file.read_text(encoding="utf-8").splitlines()
        return [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    return [
        "https://example.com/product/1",
    ]


def main() -> None:
    ns = parse_args()
    logging.basicConfig(
        level=getattr(logging, ns.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("ny_disclosure_audit")

    urls = load_urls(ns)
    cfg = AuditConfig(
        urls=urls,
        output_dir=ns.output_dir.resolve(),
        headless=not ns.headed,
        max_concurrency=ns.max_concurrency,
        capture_screenshot=not ns.no_screenshot,
        nav_timeout_ms=ns.nav_timeout_ms,
        personas=default_personas(),
    )

    asyncio.run(run_audit(cfg, log))


if __name__ == "__main__":
    main()
