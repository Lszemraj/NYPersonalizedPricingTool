"""
NY Algorithmic Pricing Disclosure Act — compliance visibility audit (research prototype).

Audits whether required disclosure text appears and quantifies accessibility metrics
(font size, contrast, placement, scroll, estimated click friction). This tool measures
disclosure visibility and prominence; it does not establish that personalized pricing occurred.

Target platforms in the built-in registry are chosen as plausible high-value environments for
disclosure/compliance review (dynamic pricing, account-based flows, checkout, booking, etc.).
Listing a platform does not assert that it uses personalized pricing or violates any law.
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
from urllib.parse import urlparse
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
NETWORKIDLE_GOTO_TIMEOUT_MS = 8_000
DEFAULT_MAX_CONCURRENCY = 2
NEAR_PRICE_VERTICAL_PX = 120
DEFAULT_MAX_EXPLORATION_DEPTH = 4
DEFAULT_MAX_CANDIDATE_CLICKS = 8
DEFAULT_TEST_ZIP = "10001"
INTERACTION_DELAY_SEC = 0.35  # ethics / rate smoothing; not anti-bot evasion

StrategyKind = Literal["food_delivery", "grocery_retail", "travel", "ticketing", "generic"]
AccountState = Literal["fresh", "returning", "high_activity"]
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
    persona_group: str = "unspecified"
    account_state: str = "fresh"

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("persona name must be non-empty")
        if self.account_state not in ("fresh", "returning", "high_activity"):
            raise ValueError("account_state must be fresh, returning, or high_activity")


def load_personas_from_json(path: Path, log: logging.Logger | None = None) -> list[PersonaConfig]:
    """
    Load persona definitions from a JSON array file.

    Each entry supports: name, user_agent, locale, timezone, geolocation {latitude, longitude},
    storage_state_path (optional string path), persona_group, account_state.
    YAML is not loaded here to avoid extra dependencies—convert YAML to JSON if needed.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Persona config must be a JSON array")
    out: list[PersonaConfig] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        geo = row.get("geolocation")
        gloc = None
        if isinstance(geo, dict):
            gloc = GeolocationConfig(float(geo["latitude"]), float(geo["longitude"]))
        ssp = row.get("storage_state_path")
        pssp = Path(ssp).expanduser().resolve() if ssp else None
        persona = PersonaConfig(
            name=str(row["name"]),
            user_agent=str(row["user_agent"]),
            locale=str(row.get("locale", "en-US")),
            timezone=str(row.get("timezone", "America/New_York")),
            geolocation=gloc,
            storage_state_path=pssp,
            persona_group=str(row.get("persona_group", "unspecified")),
            account_state=str(row.get("account_state", "fresh")),
        )
        out.append(persona)
        if log:
            log.debug("Loaded persona %s (%s / %s)", persona.name, persona.persona_group, persona.account_state)
    if not out:
        raise ValueError("No valid personas in file")
    return out


from site_registry import (
    CATEGORY_LABELS,
    SITE_REGISTRY,
    SiteProfile,
    StrategyKind,
    normalize_category_filter as _normalize_category_filter,
)


@dataclass(frozen=True)
class AuditTarget:
    """Single URL to visit, optionally tied to registry metadata."""

    url: str
    platform: SiteProfile | None


@dataclass
class AuditConfig:
    """Top-level run configuration (CLI + defaults)."""

    targets: list[AuditTarget]
    output_dir: Path
    headless: bool = True
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    capture_screenshot: bool = True
    nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS
    viewport: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_VIEWPORT))
    personas: list[PersonaConfig] = field(default_factory=list)
    max_exploration_depth: int = DEFAULT_MAX_EXPLORATION_DEPTH
    max_candidate_clicks_per_step: int = DEFAULT_MAX_CANDIDATE_CLICKS
    use_test_address: bool = False
    test_zip: str = DEFAULT_TEST_ZIP
    continue_after_disclosure: bool = False
    global_storage_state: Path | None = None


# Registry data lives in site_registry.py for maintainability.

def expand_registry_to_audit_targets(
    *,
    pilot_only: bool,
    enabled_only: bool,
    categories: frozenset[str] | None,
    platform_names: frozenset[str] | None,
) -> list[AuditTarget]:
    """Flatten registry to one AuditTarget per (platform, target URL)."""
    out: list[AuditTarget] = []
    for p in SITE_REGISTRY:
        if enabled_only and not p.enabled:
            continue
        if pilot_only and not p.pilot_priority:
            continue
        if categories is not None and p.category_slug not in categories:
            continue
        if platform_names is not None:
            if p.platform_name.strip().casefold() not in platform_names:
                continue
        for u in p.target_urls:
            out.append(AuditTarget(url=u, platform=p))
    return out


def manual_audit_targets(urls: list[str]) -> list[AuditTarget]:
    return [AuditTarget(url=u, platform=None) for u in urls]


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
    font_size_ratio_vs_body: float | None = None
    line_height_px: float | None = None
    max_z_index_in_chain: int | None = None
    stacked_under_opaque_layer: bool = False
    distance_px_from_nearest_price: float | None = None
    disclosure_surface: str | None = None  # footer, modal, accordion, iframe, main, unknown
    alongside_price_visual_group: bool | None = None
    document_highlight_rect: dict[str, float] | None = None  # left, top, width, height (document coords)


@dataclass
class DisclosureEvaluation:
    """Aggregated disclosure search: exact/normalized flags, best pick, all candidates."""

    found_exact: bool
    found_normalized: bool
    found_near_match: bool
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
class PricingBreakdown:
    """Structured price signals from DOM heuristics (research comparison; not payment truth)."""

    normalized_price_value: float | None
    currency: str
    base_price_value: float | None
    total_price_value: float | None
    cart_total_candidate: float | None
    fee_items: list[dict[str, Any]]
    primary_price_doc_rect: dict[str, float] | None


def _parse_pricing_breakdown_payload(raw: dict[str, Any] | None) -> PricingBreakdown:
    """Build a PricingBreakdown from evaluate() JSON; tolerates missing keys."""
    if not raw:
        raw = {}
    fees = list(raw.get("fee_items") or [])
    for row in fees:
        if not isinstance(row, dict):
            continue
        tx = str(row.get("text") or "")
        if row.get("value") is None:
            m = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", tx)
            if m:
                try:
                    row["value"] = float(m.group(1).replace(",", ""))
                except ValueError:
                    pass
    rect = raw.get("primary_price_doc_rect")
    if rect is not None and not isinstance(rect, dict):
        rect = None
    return PricingBreakdown(
        normalized_price_value=raw.get("normalized_price_value"),
        currency=str(raw.get("currency") or "USD"),
        base_price_value=raw.get("base_price_value"),
        total_price_value=raw.get("total_price_value"),
        cart_total_candidate=raw.get("cart_total_candidate"),
        fee_items=fees,
        primary_price_doc_rect=rect,
    )


def _merge_pricing_breakdown_dicts(chunks: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Merge per-frame pricing breakdown dicts: prefer richer totals and union fee lines.
    """
    best: dict[str, Any] | None = None
    best_score = -1.0
    all_fees: list[dict[str, Any]] = []
    seen_fee_txt: set[str] = set()
    primary_rect: dict[str, float] | None = None
    for ch in chunks:
        pb = ch.get("pricingBreakdown")
        if not isinstance(pb, dict):
            continue
        nv = pb.get("normalized_price_value")
        score = sum(
            1
            for k in ("normalized_price_value", "base_price_value", "total_price_value")
            if pb.get(k) is not None
        )
        if score > best_score:
            best_score = score
            best = dict(pb)
        elif score == best_score and best is not None and nv is not None:
            bn = best.get("normalized_price_value")
            try:
                if bn is None or float(nv) > float(bn):
                    best = dict(pb)
            except (TypeError, ValueError):
                pass
        fi = pb.get("fee_items") or []
        if isinstance(fi, list):
            for row in fi:
                if not isinstance(row, dict):
                    continue
                tx = str(row.get("text", ""))[:300]
                if tx and tx not in seen_fee_txt:
                    seen_fee_txt.add(tx)
                    all_fees.append(dict(row))
        pr = pb.get("primary_price_doc_rect")
        if isinstance(pr, dict) and primary_rect is None:
            primary_rect = dict(pr)
    if best is None:
        return {}
    best["fee_items"] = all_fees[:40]
    if primary_rect is not None:
        best["primary_price_doc_rect"] = primary_rect
    return best


@dataclass
class PageRunResult:
    """Single URL × persona audit record (flattened fields for CSV/JSON)."""

    run_id: str
    timestamp_iso: str
    url: str
    target_url: str
    platform_name: str
    category: str
    homepage_url: str
    pilot_priority: bool
    requires_login: bool
    likely_price_page_type: str
    platform_notes: str
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
    # Guided exploration & disclosure visibility (research schema)
    disclosure_found_near_match: bool = False
    page_stage: str = ""
    click_depth: int = 0
    path_taken: list[str] = field(default_factory=list)
    selectors_clicked: list[str] = field(default_factory=list)
    pricing_state_confidence: float | None = None
    likely_price_page_type_detected: str | None = None
    price_found: bool = False
    fee_candidates: list[str] = field(default_factory=list)
    first_found_stage: str | None = None
    first_found_click_depth: int | None = None
    first_found_url: str | None = None
    clicks_required_to_first_disclosure: int | None = None
    scroll_depth_to_disclosure: float | None = None
    found_in_modal: bool = False
    found_in_accordion: bool = False
    found_in_footer: bool = False
    found_after_login: bool = False
    found_after_address_entry: bool = False
    found_after_search: bool = False
    found_after_add_to_cart: bool = False
    found_after_checkout_transition: bool = False
    blocked_by_login: bool = False
    blocked_by_address_gate: bool = False
    used_storage_state: bool = False
    used_test_address: bool = False
    stop_reason: str = ""
    exploration_stages_json: list[dict[str, Any]] = field(default_factory=list)
    # Full nested payloads for JSON export
    disclosure: DisclosureEvaluation | None = None
    prices: list[PriceCandidate] = field(default_factory=list)
    persona_group: str = ""
    account_state: str = ""
    screenshot_annotated_path: str | None = None
    disclosure_font_weight: str | None = None
    disclosure_font_size_ratio_vs_body: float | None = None
    disclosure_line_height_px: float | None = None
    disclosure_distance_px_from_price: float | None = None
    disclosure_placement_surface: str | None = None
    disclosure_alongside_price_visual_group: bool | None = None
    disclosure_max_z_index: int | None = None
    disclosure_visually_obscured: bool = False
    disclosure_visible_without_interaction: bool | None = None
    disclosure_on_separate_visual_surface: bool = False  # iframe / footer / modal vs main price column
    research_timing_first_disclosure: str | None = None
    interactions_before_first_disclosure: int | None = None
    normalized_price_value: float | None = None
    currency_code: str | None = None
    pricing_base_price_value: float | None = None
    pricing_total_price_value: float | None = None
    pricing_cart_total_candidate: float | None = None
    pricing_fee_items_json: str = ""
    pricing_breakdown: PricingBreakdown | None = None


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
    """
    Navigate with pragmatic wait strategies: domcontentloaded → load → short networkidle.

    Heavy SPAs often never settle to networkidle; idle is last-resort only with a capped
    timeout so it cannot consume the full navigation budget.
    """
    strategies: list[tuple[str, str, int]] = [
        ("domcontentloaded", "domcontentloaded", timeout_ms),
        ("load", "load", timeout_ms),
        (
            "networkidle",
            "networkidle",
            min(NETWORKIDLE_GOTO_TIMEOUT_MS, timeout_ms),
        ),
    ]
    last_err: Exception | None = None
    for label, wait_until, strat_timeout in strategies:
        try:
            await page.goto(url, wait_until=wait_until, timeout=strat_timeout)
            log.debug("Loaded %s with wait_until=%s timeout_ms=%s", url, label, strat_timeout)
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

# Ordinary triple-quoted string (not raw): each JS regexp backslash is written as \\ below.
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

  function classifySurface(el) {
    let n = el;
    while (n && n !== document.body && n !== document.documentElement) {
      const tag = (n.tagName || "").toLowerCase();
      const role = n.getAttribute && n.getAttribute("role");
      if (tag === "footer") return "footer";
      if (role === "dialog" || tag === "dialog") return "modal";
      if (tag === "details") return "accordion";
      n = n.parentElement;
    }
    return inIframe ? "iframe" : "main";
  }

  function maxZInAncestors(el) {
    let zMax = 0;
    let n = el;
    while (n) {
      const z = parseInt(window.getComputedStyle(n).zIndex, 10);
      if (!isNaN(z) && z > zMax) zMax = z;
      n = n.parentElement;
    }
    return zMax;
  }

  const bodyFontPx = parseFloat(window.getComputedStyle(document.body).fontSize) || 16;
  const priceAnchors = [];
  const currencyProbe = /\\$\\s*\\d[\\d,]*(?:\\.\\d{2})?/;
  for (const pel of Array.from(document.querySelectorAll("body *")).slice(0, 260)) {
    try {
      const tx = pel.innerText || "";
      if (!currencyProbe.test(tx) || tx.length > 260) continue;
      const mr = pel.getBoundingClientRect();
      if (mr.width < 2 || mr.height < 2) continue;
      priceAnchors.push({
        cx: mr.left + mr.width / 2,
        cy: mr.top + mr.height / 2,
        docLeft: mr.left + window.scrollX,
        docTop: mr.top + window.scrollY,
        w: mr.width,
        h: mr.height,
        snippet: tx.trim().slice(0, 80),
      });
    } catch (e) {}
    if (priceAnchors.length > 48) break;
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
    const lhRaw = style.lineHeight;
    const lhPx =
      lhRaw && lhRaw.endsWith("px") && !isNaN(parseFloat(lhRaw)) ? parseFloat(lhRaw) : null;
    const ratioBody = fs && bodyFontPx ? fs / bodyFontPx : null;
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

    const ecx = rect.left + rect.width / 2;
    const ecy = rect.top + rect.height / 2;
    let minDistPx = null;
    for (const pa of priceAnchors) {
      const d = Math.hypot(ecx - pa.cx, ecy - pa.cy);
      if (minDistPx === null || d < minDistPx) minDistPx = d;
    }
    let alongside =
      minDistPx !== null ? minDistPx <= 220 : null;

    let stackedUnder = false;
    try {
      const hit = document.elementFromPoint(ecx, ecy);
      stackedUnder =
        !!(hit && hit !== el && !el.contains(hit) && !hit.contains(el));
    } catch (e) {}

    const est = estimateClicks(el);
    let clicks = est.clicks;
    let approx = est.approx;
    if (visible && clicks === 0) {
    } else if (!visible && clicks === 0) {
      clicks = 1;
      approx = true;
    }

    const hint = inIframe ? ("iframe::" + selectorHint(el)) : selectorHint(el);
    const surface = classifySurface(el);
    const zMax = maxZInAncestors(el);
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
      font_size_ratio_vs_body: ratioBody,
      line_height_px: lhPx,
      max_z_index_in_chain: zMax,
      stacked_under_opaque_layer: stackedUnder,
      distance_px_from_nearest_price: minDistPx,
      disclosure_surface: surface,
      alongside_price_visual_group: alongside,
      document_highlight_rect: {
        left: rect.left + window.scrollX,
        top: rect.top + window.scrollY,
        width: rect.width,
        height: rect.height,
      },
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

  const bl = (document.body && document.body.innerText) ? document.body.innerText.toLowerCase() : "";
  const nearMatch = (() => {
    const a = /algorithm|algorithmic/.test(bl);
    const pd = /personal(ized)?|personal data/.test(bl);
    const pr = /price|pricing/.test(bl);
    return a && pd && pr;
  })();
  const gates = {
    login_wall: /sign in (to continue| for)|log in to (see|view|continue)|create an account to/i.test(bl),
    address_gate: /enter (your )?(delivery |shipping )?address|add your address|your zip|postal code|zip code/i.test(bl),
  };
  const feeRegex = /(fee|tax|service charge|surcharge|booking fee|convenience fee)/i;
  const feeCandidates = [];
  const lines = bl.split(/\\n/).slice(0, 200);
  for (const line of lines) {
    if (feeRegex.test(line) && /\\$|€|£|\\d+\\.\\d{2}/.test(line)) {
      feeCandidates.push(line.slice(0, 200));
      if (feeCandidates.length >= 25) break;
    }
  }

  let pricingBreakdown = {
    normalized_price_value: null,
    currency: "USD",
    base_price_value: null,
    total_price_value: null,
    cart_total_candidate: null,
    fee_items: [],
    primary_price_doc_rect: null,
  };
  try {
    let maxV = -1;
    let bestAnch = null;
    for (const pa of priceAnchors) {
      const m = pa.snippet.match(/\\$\\s*([\\d,]+(?:\\.\\d{1,2})?)/);
      if (!m) continue;
      const v = parseFloat(m[1].replace(/,/g, ""));
      if (!isNaN(v) && v > maxV) {
        maxV = v;
        bestAnch = pa;
      }
    }
    const totalKw = /(total|subtotal|order total|estimated total|grand total)/i;
    let totalV = maxV >= 0 ? maxV : null;
    let cartV = null;
    if ((bodyText && totalKw.test(bodyText)) || priceAnchors.length) {
      for (const pa of priceAnchors) {
        const m = pa.snippet.match(/\\$\\s*([\\d,]+(?:\\.\\d{1,2})?)/);
        if (!m) continue;
        const v = parseFloat(m[1].replace(/,/g, ""));
        if (!isNaN(v) && (cartV === null || v > cartV)) cartV = v;
      }
      if (cartV !== null) totalV = cartV;
    }
    pricingBreakdown = {
      normalized_price_value: totalV !== null ? totalV : (maxV >= 0 ? maxV : null),
      currency: "USD",
      base_price_value: maxV >= 0 ? maxV : null,
      total_price_value: totalV !== null ? totalV : null,
      cart_total_candidate: cartV,
      fee_items: feeCandidates.slice(0, 20).map((t) => ({ text: String(t).slice(0, 260), value: null })),
      primary_price_doc_rect:
        bestAnch != null
          ? { left: bestAnch.docLeft, top: bestAnch.docTop, width: bestAnch.w, height: bestAnch.h }
          : null,
    };
  } catch (e) {}

  const scrollHeight = Math.max(
    document.documentElement ? document.documentElement.scrollHeight : 0,
    document.body ? document.body.scrollHeight : 0,
    0
  );

  return {
    candidates,
    matchCount: matching.length,
    priceCandidates,
    nearMatch,
    gates,
    feeCandidates,
    pricingBreakdown,
    scrollHeight,
    viewportHeight: window.innerHeight,
    pricingStateScore: Math.min(1, (priceCandidates.length > 3 ? 0.4 : 0.1) + (feeCandidates.length ? 0.3 : 0) + (priceHits.length ? 0.3 : 0)),
    likelyPageType: (() => {
      const u = (window.location && window.location.href) ? window.location.href.toLowerCase() : "";
      if (u.includes("cart") || u.includes("basket") || u.includes("bag")) return "cart";
      if (u.includes("checkout") || u.includes("payment") || u.includes("/pay")) return "checkout";
      if (u.includes("search") || u.includes("results")) return "search_results";
      if (u.includes("flight") || u.includes("hotel") || u.includes("listing") || u.includes("product")) return "listing_or_product";
      return "other";
    })(),
  };
}
"""


def _merge_frame_evaluations(chunks: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge per-frame evaluate() payloads; dedupe price rows later in Python."""
    cands: list[Any] = []
    prices_raw: list[Any] = []
    near = False
    gates_m = {"login_wall": False, "address_gate": False}
    fees: list[str] = []
    scores: list[float] = []
    page_types: list[str] = []
    scroll_heights: list[float] = []
    viewports: list[float] = []
    for ch in chunks:
        cands.extend(ch.get("candidates") or [])
        prices_raw.extend(ch.get("priceCandidates") or [])
        near = near or bool(ch.get("nearMatch"))
        g = ch.get("gates") or {}
        gates_m["login_wall"] = gates_m["login_wall"] or bool(g.get("login_wall"))
        gates_m["address_gate"] = gates_m["address_gate"] or bool(g.get("address_gate"))
        fees.extend(ch.get("feeCandidates") or [])
        sc = ch.get("pricingStateScore")
        if sc is not None:
            scores.append(float(sc))
        pt = ch.get("likelyPageType")
        if pt and pt != "other":
            page_types.append(str(pt))
        sh = ch.get("scrollHeight")
        if sh is not None:
            try:
                scroll_heights.append(float(sh))
            except (TypeError, ValueError):
                pass
        vh = ch.get("viewportHeight")
        if vh is not None:
            try:
                viewports.append(float(vh))
            except (TypeError, ValueError):
                pass

    pb_merged = _merge_pricing_breakdown_dicts(chunks)

    return {
        "candidates": cands,
        "matchCount": len(cands),
        "priceCandidates": prices_raw,
        "nearMatch": near,
        "gates": gates_m,
        "feeCandidates": fees[:50],
        "pricingStateScore": max(scores) if scores else 0.0,
        "likelyPageType": page_types[0] if page_types else "other",
        "pricingBreakdown": pb_merged,
        "scrollHeight": max(scroll_heights) if scroll_heights else 0.0,
        "viewportHeight": max(viewports) if viewports else 0.0,
    }


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
        dhr = c.get("document_highlight_rect")
        if dhr is not None and not isinstance(dhr, dict):
            dhr = None
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
                font_size_ratio_vs_body=c.get("font_size_ratio_vs_body"),
                line_height_px=c.get("line_height_px"),
                max_z_index_in_chain=c.get("max_z_index_in_chain"),
                stacked_under_opaque_layer=bool(c.get("stacked_under_opaque_layer")),
                distance_px_from_nearest_price=c.get("distance_px_from_nearest_price"),
                disclosure_surface=c.get("disclosure_surface"),
                alongside_price_visual_group=c.get("alongside_price_visual_group"),
                document_highlight_rect=dhr,
            )
        )

    found_exact = any(p.found_exact for p in parsed)
    found_norm = any(p.found_normalized for p in parsed)
    found_near = bool(raw.get("nearMatch"))
    best = _pick_best_disclosure(parsed)
    matched = best.matched_text[:200] if best else None
    return DisclosureEvaluation(
        found_exact=found_exact,
        found_normalized=found_norm,
        found_near_match=found_near,
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
        stacked_pen = 1 if not c.stacked_under_opaque_layer else 0
        ratio = c.font_size_ratio_vs_body or 0.0
        ratio_capped = min(max(ratio, 0.0), 3.0) / 3.0
        return (vis, stacked_pen, cr, ratio_capped, -dist, clicks_pen)

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
# Category-aware exploration (bounded; disclosure/compliance visibility only)
# ---------------------------------------------------------------------------

BAD_CLICK_SUBSTRINGS = (
    "sign out",
    "log out",
    "delete",
    "remove card",
    "close account",
    "cancel subscription",
)

KEYWORDS_BY_STRATEGY: dict[str, list[tuple[str, int]]] = {
    "food_delivery": [
        ("View cart", 12),
        ("Cart", 11),
        ("Checkout", 10),
        ("Continue", 8),
        ("Add", 6),
        ("See menu", 5),
        ("Start order", 9),
    ],
    "grocery_retail": [
        ("Add to cart", 12),
        ("Cart", 11),
        ("Checkout", 10),
        ("View cart", 11),
        ("Continue", 8),
        ("Place order", 9),
    ],
    "travel": [
        ("Search", 9),
        ("Continue", 8),
        ("Select", 7),
        ("Book", 10),
        ("Reserve", 9),
        ("View deal", 6),
    ],
    "ticketing": [
        ("Tickets", 11),
        ("Select", 9),
        ("Continue", 8),
        ("Checkout", 10),
        ("View seats", 7),
    ],
    "generic": [
        ("Cart", 9),
        ("Checkout", 10),
        ("Continue", 7),
        ("View", 5),
    ],
}


def _infer_page_stage(url: str, likely_js: str) -> str:
    u = url.lower()
    if "cart" in u or "basket" in u or "bag" in u:
        return "cart"
    if "checkout" in u or "/pay" in u or "payment" in u:
        return "checkout_summary"
    if "search" in u or "results" in u or "find" in u:
        return "search_results"
    if likely_js and likely_js != "other":
        return f"page_{likely_js}"
    return "homepage_or_content"


def normalized_target_url(url: str) -> str:
    """
    Canonical key for persona comparison: scheme + netloc + path (no trailing slash, no fragment).
    """
    try:
        p = urlparse(url.strip())
        path = (p.path or "").rstrip("/") or ""
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        return f"{scheme}://{netloc}{path}"
    except Exception:
        return url.strip().casefold()


def research_timing_bucket(url: str, page_stage: str) -> str:
    """Coarse funnel label for statutory disclosure timing (research coding)."""
    u = url.lower()
    if "checkout" in u or "payment" in u or "/pay" in u or page_stage.startswith("checkout"):
        return "checkout"
    if "cart" in u or "basket" in u or "bag" in u or page_stage == "cart":
        return "cart"
    if "search" in u or "results" in u or "find" in u or page_stage == "search_results":
        return "search_results"
    if "listing_or_product" in page_stage or "product" in u or "/p/" in u or "/item" in u:
        return "product_page"
    if page_stage in ("homepage_or_content", "") and u.count("/") <= 3:
        return "landing_page"
    return "other"


def estimate_scroll_fraction_to_element(
    doc_top_px: float | None, scroll_height: float, viewport_h: float
) -> float | None:
    """
    Rough scroll depth estimate: disclosure offset / scrollable distance (0–1 when scrollable exists).
    """
    if doc_top_px is None or viewport_h <= 0:
        return None
    denom = max(float(scroll_height) - viewport_h, 1.0)
    return max(0.0, min(1.0, float(doc_top_px) / denom))


async def inject_document_overlay_boxes(page: Page, disc_rect: dict[str, Any], price_rect: dict[str, Any] | None) -> None:
    """Draw transient full-page overlays in the main frame (document-coordinate rects)."""
    pr = price_rect if isinstance(price_rect, dict) else None
    await page.evaluate(
        """async ({ disc, price }) => {
      const nid = '__ny_audit_hl__';
      document.getElementById(nid)?.remove();
      const sw = Math.max(document.documentElement.scrollWidth, document.body.scrollWidth);
      const sh = Math.max(document.documentElement.scrollHeight, document.body.scrollHeight);
      const holder = document.createElement('div');
      holder.id = nid;
      holder.style.position = 'absolute';
      holder.style.left = '0';
      holder.style.top = '0';
      holder.style.width = sw + 'px';
      holder.style.height = sh + 'px';
      holder.style.pointerEvents = 'none';
      holder.style.zIndex = '2147483647';
      function box(r, color) {
        if (!r || !r.width || !r.height) return;
        const d = document.createElement('div');
        d.style.position = 'absolute';
        d.style.left = r.left + 'px';
        d.style.top = r.top + 'px';
        d.style.width = r.width + 'px';
        d.style.height = r.height + 'px';
        d.style.boxSizing = 'border-box';
        d.style.border = '3px solid ' + color;
        d.style.background = 'rgba(255,255,255,0.02)';
        holder.appendChild(d);
      }
      document.body.appendChild(holder);
      box(disc, '#c62828');
      box(price || {}, '#1565c0');
    }""",
        {"disc": disc_rect, "price": pr or {}},
    )


async def clear_document_overlay_boxes(page: Page) -> None:
    await page.evaluate("() => document.getElementById('__ny_audit_hl__')?.remove()")


async def _try_fill_test_zip(page: Page, zip_code: str, log: logging.Logger) -> bool:
    selectors = [
        'input[placeholder*="zip" i]',
        'input[name*="postal" i]',
        'input[autocomplete="postal-code"]',
        'input[id*="zip" i]',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() == 0:
                continue
            if await loc.is_visible(timeout=600):
                await loc.fill(zip_code)
                log.info("Filled test ZIP in %s", sel)
                await asyncio.sleep(max(INTERACTION_DELAY_SEC, 0.35))
                return True
        except PlaywrightError:
            continue
    return False


def _better_disclosure(
    a: DisclosureEvaluation | None, b: DisclosureEvaluation
) -> DisclosureEvaluation:
    if a is None:
        return b
    if b.found_exact and not a.found_exact:
        return b
    if b.found_normalized and not a.found_normalized and not a.found_exact:
        return b
    if b.found_near_match and not a.found_near_match and not (a.found_exact or a.found_normalized):
        return b
    return a


async def _pick_next_exploration_click(
    page: Page,
    strategy: StrategyKind,
    clicked: set[str],
    max_rank: int,
    log: logging.Logger,
) -> tuple[str, Any] | None:
    keys = KEYWORDS_BY_STRATEGY.get(strategy, KEYWORDS_BY_STRATEGY["generic"])
    ranked: list[tuple[float, str, Any]] = []
    for text, base in keys:
        try:
            pat = re.compile(re.escape(text), re.I)
            btn = page.get_by_role("button", name=pat)
            lk = page.get_by_role("link", name=pat)
            for loc_name, loc in (("btn", btn), ("link", lk)):
                n = await loc.count()
                for i in range(min(n, 3)):
                    el = loc.nth(i)
                    if not await el.is_visible(timeout=400):
                        continue
                    try:
                        inner = (await el.inner_text())[:120]
                    except Exception:
                        inner = text
                    low = inner.lower()
                    if any(b in low for b in BAD_CLICK_SUBSTRINGS):
                        continue
                    sig = f"{loc_name}:{text}:{inner[:50]}"
                    if sig in clicked:
                        continue
                    ranked.append((float(base), sig, el))
        except Exception:
            continue
    ranked.sort(key=lambda x: -x[0])
    for _, sig, el in ranked[:max_rank]:
        return (sig, el)
    return None


async def guided_exploration_loop(
    page: Page,
    start_url: str,
    site: SiteProfile | None,
    cfg: AuditConfig,
    log: logging.Logger,
) -> dict[str, Any]:
    """
    Multi-step bounded navigation toward pricing-relevant surfaces. Does not submit payment.
    """
    strat: StrategyKind = site.strategy_kind if site else "generic"
    path: list[str] = [start_url]
    selectors_clicked: list[str] = []
    clicked: set[str] = set()
    stages: list[dict[str, Any]] = []
    best_disc: DisclosureEvaluation | None = None
    best_prices: list[PriceCandidate] = []
    best_fees: list[str] = []
    best_score = 0.0
    best_likely_type = "other"
    stop_reason: str = "completed"
    disclosure_step_index: int | None = None
    disclosure_url: str | None = None
    first_disc_depth: int | None = None
    disclosure_visible_without_interaction: bool | None = None
    scroll_depth_at_first_disclosure: float | None = None
    research_timing_first_disclosure: str | None = None
    all_pb_rounds: list[dict[str, Any]] = []
    blocked_login = False
    blocked_addr = False
    used_zip = False
    err: str | None = None

    try:
        await robust_goto(page, start_url, cfg.nav_timeout_ms, log)
        await asyncio.sleep(0.45)
        await dismiss_common_overlays(page, log)

        for depth in range(cfg.max_exploration_depth + 1):
            raw = await evaluate_disclosure_all_frames(page, log)
            disc = _parse_disclosure_js(raw)
            prices = _parse_prices(raw.get("priceCandidates") or [])
            fees = list(raw.get("feeCandidates") or [])
            gates = raw.get("gates") or {}
            score = float(raw.get("pricingStateScore") or 0.0)
            likely = str(raw.get("likelyPageType") or "other")
            pb_raw = raw.get("pricingBreakdown")
            if isinstance(pb_raw, dict) and pb_raw:
                all_pb_rounds.append(pb_raw)
            cur_url = page.url
            stage = _infer_page_stage(cur_url, likely)

            blocked_login = blocked_login or bool(gates.get("login_wall"))
            blocked_addr = blocked_addr or bool(gates.get("address_gate"))

            if cfg.use_test_address and site and (site.address_required or gates.get("address_gate")):
                used_zip = used_zip or await _try_fill_test_zip(page, cfg.test_zip, log)

            stages.append(
                {
                    "depth": depth,
                    "url": cur_url,
                    "page_stage": stage,
                    "disclosure_exact": disc.found_exact,
                    "disclosure_normalized": disc.found_normalized,
                    "disclosure_near": disc.found_near_match,
                    "price_candidates_n": len(prices),
                    "pricing_state_confidence": score,
                    "likely_price_page_type_detected": likely,
                    "gates": gates,
                }
            )

            if disc.found_exact or disc.found_normalized:
                if disclosure_step_index is None:
                    disclosure_step_index = depth
                    disclosure_url = cur_url
                    first_disc_depth = depth
                    bc0 = disc.best_candidate
                    if depth == 0 and bc0 is not None:
                        disclosure_visible_without_interaction = bool(bc0.visible_in_viewport)
                    else:
                        disclosure_visible_without_interaction = False
                    sh = float(raw.get("scrollHeight") or 0)
                    vh = float(raw.get("viewportHeight") or 0) or 800.0
                    if bc0 is not None:
                        scroll_depth_at_first_disclosure = estimate_scroll_fraction_to_element(
                            bc0.distance_from_document_top_px, sh, vh
                        )
                    research_timing_first_disclosure = research_timing_bucket(cur_url, stage)
            best_disc = _better_disclosure(best_disc, disc)
            if score >= best_score:
                best_score = score
                best_prices = prices
                best_fees = fees
                best_likely_type = likely
            elif not best_prices and prices:
                best_prices = prices
                best_fees = fees

            if disc.found_exact or disc.found_normalized:
                if not cfg.continue_after_disclosure:
                    stop_reason = "disclosure_found"
                    break
            if score >= 0.65 and depth > 0:
                stop_reason = "strong_pricing_state"
                if not (disc.found_exact or disc.found_normalized):
                    break
            if depth >= cfg.max_exploration_depth:
                stop_reason = "max_depth_reached"
                break
            pick = await _pick_next_exploration_click(
                page,
                strat,
                clicked,
                cfg.max_candidate_clicks_per_step,
                log,
            )
            if not pick:
                stop_reason = "no_safe_candidates"
                break
            sig, el = pick
            try:
                await el.click(timeout=4000)
                clicked.add(sig)
                selectors_clicked.append(sig)
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                await asyncio.sleep(max(0.5, INTERACTION_DELAY_SEC))
                path.append(page.url)
            except Exception as e:
                log.debug("Exploration click failed: %s", e)
                stop_reason = "navigation_failure"
                err = str(e)
                break

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        stop_reason = "error"
        log.exception("guided_exploration_loop")

    if stop_reason == "completed" and not err:
        stop_reason = "max_depth_reached" if stages else "completed"

    merged_pb = _merge_pricing_breakdown_dicts(all_pb_rounds) if all_pb_rounds else {}

    return {
        "best_disc": best_disc,
        "best_prices": best_prices,
        "fee_candidates": best_fees,
        "pricing_state_confidence": best_score,
        "likely_price_page_type_detected": best_likely_type,
        "path_taken": path,
        "selectors_clicked": selectors_clicked,
        "click_depth": len(selectors_clicked),
        "page_stage": _infer_page_stage(page.url, best_likely_type),
        "stages": stages,
        "stop_reason": stop_reason,
        "first_found_stage": stages[disclosure_step_index]["page_stage"] if disclosure_step_index is not None and disclosure_step_index < len(stages) else None,
        "first_found_url": disclosure_url,
        "first_found_click_depth": first_disc_depth,
        "clicks_to_disclosure": first_disc_depth,
        "blocked_by_login": blocked_login,
        "blocked_by_address_gate": blocked_addr,
        "used_test_address": used_zip,
        "error": err,
        "pricing_breakdown": merged_pb,
        "disclosure_visible_without_interaction": disclosure_visible_without_interaction,
        "scroll_depth_to_disclosure": scroll_depth_at_first_disclosure,
        "research_timing_first_disclosure": research_timing_first_disclosure,
    }


# ---------------------------------------------------------------------------
# Core audit step
# ---------------------------------------------------------------------------


def _empty_platform_fields() -> dict[str, Any]:
    return {
        "platform_name": "",
        "category": "",
        "homepage_url": "",
        "pilot_priority": False,
        "requires_login": False,
        "likely_price_page_type": "manual_or_unknown",
        "platform_notes": "",
    }


def _platform_row_fields(platform: SiteProfile | None) -> dict[str, Any]:
    if platform is None:
        return _empty_platform_fields()
    return {
        "platform_name": platform.platform_name,
        "category": platform.category,
        "homepage_url": platform.homepage_url,
        "pilot_priority": platform.pilot_priority,
        "requires_login": platform.requires_login,
        "likely_price_page_type": platform.likely_price_page_types,
        "platform_notes": platform.notes,
    }


def _surface_flags_from_candidate(bc: DisclosureCandidate | None) -> tuple[bool, bool, bool, bool]:
    """
    Derive placement buckets from JS surface classification (not fixed selectors).
    Returns: modal, accordion, footer, separate_surface (iframe or separate_page).
    """
    if not bc:
        return False, False, False, False
    surf = (bc.disclosure_surface or "").lower()
    modal = surf == "modal"
    acc = surf == "accordion"
    foot = surf == "footer"
    separate = surf == "iframe" or bc.position_relative_to_price == "separate_page"
    return modal, acc, foot, separate


def make_error_result(
    *,
    run_id: str,
    url: str,
    target_url: str,
    persona: PersonaConfig,
    platform: SiteProfile | None,
    cfg: AuditConfig,
    error_msg: str,
) -> PageRunResult:
    """Minimal PageRunResult when a task fails before/with an unrecoverable audit error."""
    ts = datetime.now(timezone.utc).isoformat()
    pf = _platform_row_fields(platform)
    storage_used = bool(
        cfg.global_storage_state and cfg.global_storage_state.is_file()
    ) or bool(persona.storage_state_path and persona.storage_state_path.is_file())
    pbd = _parse_pricing_breakdown_payload({})
    empty_disc = DisclosureEvaluation(
        found_exact=False,
        found_normalized=False,
        found_near_match=False,
        matched_text=None,
        number_of_matches=0,
        best_candidate=None,
        all_candidates=[],
    )
    return PageRunResult(
        run_id=run_id,
        timestamp_iso=ts,
        url=url,
        target_url=target_url,
        platform_name=pf["platform_name"],
        category=pf["category"],
        homepage_url=pf["homepage_url"],
        pilot_priority=pf["pilot_priority"],
        requires_login=pf["requires_login"],
        likely_price_page_type=pf["likely_price_page_type"],
        platform_notes=pf["platform_notes"],
        persona_name=persona.name,
        disclosure_found_exact=False,
        disclosure_found_normalized=False,
        disclosure_visible=False,
        disclosure_font_size_px=None,
        disclosure_contrast_ratio=None,
        disclosure_clicks_required=0,
        disclosure_position_relative_to_price="unknown",
        disclosure_distance_from_top=None,
        disclosure_requires_scroll=False,
        disclosure_match_count=0,
        top_price_text=None,
        num_price_candidates=0,
        screenshot_path=None,
        html_path=None,
        json_path=None,
        error=error_msg,
        disclosure_found_near_match=False,
        page_stage="",
        click_depth=0,
        path_taken=[],
        selectors_clicked=[],
        pricing_state_confidence=None,
        likely_price_page_type_detected=None,
        price_found=False,
        fee_candidates=[],
        first_found_stage=None,
        first_found_click_depth=None,
        first_found_url=None,
        clicks_required_to_first_disclosure=None,
        scroll_depth_to_disclosure=None,
        found_in_modal=False,
        found_in_accordion=False,
        found_in_footer=False,
        found_after_login=False,
        found_after_address_entry=False,
        found_after_search=False,
        found_after_add_to_cart=False,
        found_after_checkout_transition=False,
        blocked_by_login=False,
        blocked_by_address_gate=False,
        used_storage_state=storage_used,
        used_test_address=False,
        stop_reason="",
        exploration_stages_json=[],
        disclosure=empty_disc,
        prices=[],
        persona_group=getattr(persona, "persona_group", "") or "",
        account_state=getattr(persona, "account_state", "") or "",
        screenshot_annotated_path=None,
        disclosure_font_weight=None,
        disclosure_font_size_ratio_vs_body=None,
        disclosure_line_height_px=None,
        disclosure_distance_px_from_price=None,
        disclosure_placement_surface=None,
        disclosure_alongside_price_visual_group=None,
        disclosure_max_z_index=None,
        disclosure_visually_obscured=False,
        disclosure_visible_without_interaction=None,
        disclosure_on_separate_visual_surface=False,
        research_timing_first_disclosure=None,
        interactions_before_first_disclosure=None,
        normalized_price_value=pbd.normalized_price_value,
        currency_code=pbd.currency,
        pricing_base_price_value=pbd.base_price_value,
        pricing_total_price_value=pbd.total_price_value,
        pricing_cart_total_candidate=pbd.cart_total_candidate,
        pricing_fee_items_json=json.dumps(pbd.fee_items, ensure_ascii=False),
        pricing_breakdown=pbd,
    )


async def audit_page(
    page: Page,
    url: str,
    persona: PersonaConfig,
    run_id: str,
    cfg: AuditConfig,
    log: logging.Logger,
    platform: SiteProfile | None = None,
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
    pf = _platform_row_fields(platform)
    if platform:
        pslug = re.sub(r"[^A-Za-z0-9._-]+", "_", platform.platform_name)
        base_name = f"{pslug}__{slug}__{safe}"
    else:
        base_name = f"{slug}__{safe}"
    run_dir = ensure_run_dir(cfg.output_dir, run_id)
    storage_used = bool(
        cfg.global_storage_state and cfg.global_storage_state.is_file()
    ) or bool(persona.storage_state_path and persona.storage_state_path.is_file())

    geo: dict[str, Any] = {}
    pbd = _parse_pricing_breakdown_payload({})
    screenshot_annotated_path: str | None = None
    try:
        geo = await guided_exploration_loop(page, url, platform, cfg, log)
        disc = geo.get("best_disc")
        prices = geo.get("best_prices") or []
        err = geo.get("error")
        if disc is None:
            disc = DisclosureEvaluation(
                found_exact=False,
                found_normalized=False,
                found_near_match=False,
                matched_text=None,
                number_of_matches=0,
                best_candidate=None,
                all_candidates=[],
            )

        pbd = _parse_pricing_breakdown_payload(geo.get("pricing_breakdown") or {})

        html_file = run_dir / f"{base_name}.html"
        write_text_atomic(html_file, await page.content())
        html_path = str(html_file)

        if cfg.capture_screenshot:
            png = run_dir / f"{base_name}.png"
            await page.screenshot(path=str(png), full_page=True)
            screenshot_path = str(png)
            bc_ann = disc.best_candidate if disc else None
            if (
                bc_ann
                and bc_ann.document_highlight_rect
                and not (bc_ann.dom_selector_hint or "").startswith("iframe::")
            ):
                try:
                    await inject_document_overlay_boxes(
                        page,
                        bc_ann.document_highlight_rect,
                        pbd.primary_price_doc_rect,
                    )
                    ap = run_dir / f"{base_name}__annotated.png"
                    await page.screenshot(path=str(ap), full_page=True)
                    screenshot_annotated_path = str(ap)
                except Exception as ann_exc:
                    log.debug("Annotated screenshot skipped: %s", ann_exc)
                finally:
                    try:
                        await clear_document_overlay_boxes(page)
                    except Exception:
                        pass

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        log.exception("Audit failed for %s (%s)", url, persona.name)
        if disc is None:
            disc = DisclosureEvaluation(
                found_exact=False,
                found_normalized=False,
                found_near_match=False,
                matched_text=None,
                number_of_matches=0,
                best_candidate=None,
                all_candidates=[],
            )
        pbd = _parse_pricing_breakdown_payload((geo or {}).get("pricing_breakdown") or {})

    bc = disc.best_candidate if disc else None
    top_price = prices[0].raw_text if prices else None
    modal, acc, foot, separate_surf = _surface_flags_from_candidate(bc)
    clicks_to_first = geo.get("clicks_to_disclosure")
    if isinstance(clicks_to_first, int):
        clicks_req = clicks_to_first + (bc.clicks_required if bc else 0)
    else:
        clicks_req = bc.clicks_required if bc else 0

    result = PageRunResult(
        run_id=run_id,
        timestamp_iso=ts,
        url=url,
        target_url=url,
        platform_name=pf["platform_name"],
        category=pf["category"],
        homepage_url=pf["homepage_url"],
        pilot_priority=pf["pilot_priority"],
        requires_login=pf["requires_login"],
        likely_price_page_type=pf["likely_price_page_type"],
        platform_notes=pf["platform_notes"],
        persona_name=persona.name,
        disclosure_found_exact=disc.found_exact if disc else False,
        disclosure_found_normalized=disc.found_normalized if disc else False,
        disclosure_visible=bool(bc and bc.visible_in_viewport) if bc else False,
        disclosure_font_size_px=bc.font_size_px if bc else None,
        disclosure_contrast_ratio=bc.contrast_ratio if bc else None,
        disclosure_clicks_required=clicks_req,
        disclosure_position_relative_to_price=(bc.position_relative_to_price if bc else "unknown"),
        disclosure_distance_from_top=bc.distance_from_document_top_px if bc else None,
        disclosure_requires_scroll=bool(bc and bc.requires_scroll) if bc else False,
        disclosure_match_count=disc.number_of_matches if disc else 0,
        top_price_text=top_price,
        num_price_candidates=len(prices),
        screenshot_path=screenshot_path,
        html_path=html_path,
        json_path=json_path,
        error=err or geo.get("error"),
        disclosure_found_near_match=disc.found_near_match if disc else False,
        page_stage=str(geo.get("page_stage") or ""),
        click_depth=int(geo.get("click_depth") or 0),
        path_taken=list(geo.get("path_taken") or []),
        selectors_clicked=list(geo.get("selectors_clicked") or []),
        pricing_state_confidence=geo.get("pricing_state_confidence"),
        likely_price_page_type_detected=geo.get("likely_price_page_type_detected"),
        price_found=len(prices) > 0,
        fee_candidates=list(geo.get("fee_candidates") or []),
        first_found_stage=geo.get("first_found_stage"),
        first_found_click_depth=geo.get("first_found_click_depth"),
        first_found_url=geo.get("first_found_url"),
        clicks_required_to_first_disclosure=clicks_to_first,
        scroll_depth_to_disclosure=geo.get("scroll_depth_to_disclosure"),
        found_in_modal=modal,
        found_in_accordion=acc,
        found_in_footer=foot,
        found_after_login=False,
        found_after_address_entry=bool(geo.get("used_test_address")),
        found_after_search=any("search" in (u or "").lower() for u in (geo.get("path_taken") or [])),
        found_after_add_to_cart=any("cart" in (u or "").lower() for u in (geo.get("path_taken") or [])),
        found_after_checkout_transition=any(
            "checkout" in (u or "").lower() or "pay" in (u or "").lower()
            for u in (geo.get("path_taken") or [])
        ),
        blocked_by_login=bool(geo.get("blocked_by_login")),
        blocked_by_address_gate=bool(geo.get("blocked_by_address_gate")),
        used_storage_state=storage_used,
        used_test_address=bool(geo.get("used_test_address")),
        stop_reason=str(geo.get("stop_reason") or ""),
        exploration_stages_json=list(geo.get("stages") or []),
        disclosure=disc,
        prices=prices,
        persona_group=getattr(persona, "persona_group", "") or "",
        account_state=getattr(persona, "account_state", "") or "",
        screenshot_annotated_path=screenshot_annotated_path,
        disclosure_font_weight=bc.font_weight if bc else None,
        disclosure_font_size_ratio_vs_body=bc.font_size_ratio_vs_body if bc else None,
        disclosure_line_height_px=bc.line_height_px if bc else None,
        disclosure_distance_px_from_price=bc.distance_px_from_nearest_price if bc else None,
        disclosure_placement_surface=bc.disclosure_surface if bc else None,
        disclosure_alongside_price_visual_group=bc.alongside_price_visual_group if bc else None,
        disclosure_max_z_index=bc.max_z_index_in_chain if bc else None,
        disclosure_visually_obscured=bool(bc.stacked_under_opaque_layer) if bc else False,
        disclosure_visible_without_interaction=geo.get("disclosure_visible_without_interaction"),
        disclosure_on_separate_visual_surface=separate_surf,
        research_timing_first_disclosure=geo.get("research_timing_first_disclosure"),
        interactions_before_first_disclosure=geo.get("first_found_click_depth"),
        normalized_price_value=pbd.normalized_price_value,
        currency_code=pbd.currency,
        pricing_base_price_value=pbd.base_price_value,
        pricing_total_price_value=pbd.total_price_value,
        pricing_cart_total_candidate=pbd.cart_total_candidate,
        pricing_fee_items_json=json.dumps(pbd.fee_items, ensure_ascii=False),
        pricing_breakdown=pbd,
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
        "target_url": r.target_url,
        "platform_name": r.platform_name,
        "category": r.category,
        "homepage_url": r.homepage_url,
        "pilot_priority": r.pilot_priority,
        "requires_login": r.requires_login,
        "likely_price_page_type": r.likely_price_page_type,
        "platform_notes": r.platform_notes,
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
    out["disclosure_found_near_match"] = r.disclosure_found_near_match
    out["page_stage"] = r.page_stage
    out["click_depth"] = r.click_depth
    out["path_taken"] = r.path_taken
    out["selectors_clicked"] = r.selectors_clicked
    out["pricing_state_confidence"] = r.pricing_state_confidence
    out["likely_price_page_type_detected"] = r.likely_price_page_type_detected
    out["price_found"] = r.price_found
    out["fee_candidates"] = r.fee_candidates
    out["first_found_stage"] = r.first_found_stage
    out["first_found_click_depth"] = r.first_found_click_depth
    out["first_found_url"] = r.first_found_url
    out["clicks_required_to_first_disclosure"] = r.clicks_required_to_first_disclosure
    out["scroll_depth_to_disclosure"] = r.scroll_depth_to_disclosure
    out["found_in_modal"] = r.found_in_modal
    out["found_in_accordion"] = r.found_in_accordion
    out["found_in_footer"] = r.found_in_footer
    out["blocked_by_login"] = r.blocked_by_login
    out["blocked_by_address_gate"] = r.blocked_by_address_gate
    out["used_storage_state"] = r.used_storage_state
    out["used_test_address"] = r.used_test_address
    out["stop_reason"] = r.stop_reason
    out["exploration_stages"] = r.exploration_stages_json
    out["persona_group"] = r.persona_group
    out["account_state"] = r.account_state
    out["screenshot_annotated_path"] = r.screenshot_annotated_path
    out["disclosure_font_weight"] = r.disclosure_font_weight
    out["disclosure_font_size_ratio_vs_body"] = r.disclosure_font_size_ratio_vs_body
    out["disclosure_line_height_px"] = r.disclosure_line_height_px
    out["disclosure_distance_px_from_price"] = r.disclosure_distance_px_from_price
    out["disclosure_placement_surface"] = r.disclosure_placement_surface
    out["disclosure_alongside_price_visual_group"] = r.disclosure_alongside_price_visual_group
    out["disclosure_max_z_index"] = r.disclosure_max_z_index
    out["disclosure_visually_obscured"] = r.disclosure_visually_obscured
    out["disclosure_visible_without_interaction"] = r.disclosure_visible_without_interaction
    out["disclosure_on_separate_visual_surface"] = r.disclosure_on_separate_visual_surface
    out["research_timing_first_disclosure"] = r.research_timing_first_disclosure
    out["interactions_before_first_disclosure"] = r.interactions_before_first_disclosure
    out["normalized_price_value"] = r.normalized_price_value
    out["currency_code"] = r.currency_code
    out["pricing_base_price_value"] = r.pricing_base_price_value
    out["pricing_total_price_value"] = r.pricing_total_price_value
    out["pricing_cart_total_candidate"] = r.pricing_cart_total_candidate
    out["pricing_fee_items"] = r.pricing_breakdown.fee_items if r.pricing_breakdown else []
    out["gating_flags"] = {
        "blocked_by_login": r.blocked_by_login,
        "blocked_by_address_gate": r.blocked_by_address_gate,
        "used_test_address": r.used_test_address,
        "used_storage_state": r.used_storage_state,
    }
    out["pricing_breakdown"] = asdict(r.pricing_breakdown) if r.pricing_breakdown else None
    if r.disclosure:
        out["disclosure"] = {
            "found_exact": r.disclosure.found_exact,
            "found_normalized": r.disclosure.found_normalized,
            "found_near_match": r.disclosure.found_near_match,
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
    storage = cfg.global_storage_state or persona.storage_state_path
    if storage and storage.is_file():
        opts["storage_state"] = str(storage)

    return await browser.new_context(**opts)


def write_persona_differences_csv(
    path: Path, results: list[PageRunResult], log: logging.Logger
) -> None:
    """
    Compare runs that share normalized target URL and platform across personas.

    Writes boolean flags where any pairwise attribute differs among the group's personas.
    """
    groups: dict[tuple[str, str], list[PageRunResult]] = {}
    for r in results:
        nu = normalized_target_url(r.target_url or r.url)
        plat = (r.platform_name or "").strip().casefold()
        groups.setdefault((nu, plat), []).append(r)

    rows_out: list[dict[str, Any]] = []
    for (norm_url, plat), grp in sorted(groups.items()):
        if len(grp) < 2:
            continue
        names = "|".join(sorted({x.persona_name for x in grp}))
        pres = {bool(x.disclosure_found_exact or x.disclosure_found_normalized) for x in grp}
        vis = {bool(x.disclosure_visible) for x in grp}
        clicks = {int(x.disclosure_clicks_required) for x in grp}
        place = {
            (
                x.disclosure_position_relative_to_price,
                (x.disclosure_placement_surface or "").lower(),
                x.disclosure_alongside_price_visual_group,
                x.disclosure_on_separate_visual_surface,
            )
            for x in grp
        }
        price_sig: set[Any] = set()
        for x in grp:
            v = x.normalized_price_value
            price_sig.add(round(float(v), 2) if v is not None else None)
        pconf = {round(float(x.pricing_state_confidence or 0.0), 3) for x in grp}
        rows_out.append(
            {
                "normalized_url": norm_url,
                "platform_name": plat,
                "personas_compared": names,
                "n_personas": len(grp),
                "disclosure_present_diff": len(pres) > 1,
                "visibility_diff": len(vis) > 1,
                "clicks_diff": len(clicks) > 1,
                "placement_diff": len(place) > 1,
                "price_diff": len(price_sig) > 1,
                "pricing_state_confidence_diff": len(pconf) > 1,
            }
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "normalized_url",
        "platform_name",
        "personas_compared",
        "n_personas",
        "disclosure_present_diff",
        "visibility_diff",
        "clicks_diff",
        "placement_diff",
        "price_diff",
        "pricing_state_confidence_diff",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows_out:
            w.writerow(row)
    log.info("Wrote persona comparison CSV: %s", path)


def write_aggregated_summary_json(path: Path, results: list[PageRunResult], log: logging.Logger) -> None:
    """Run-level aggregates for quick research dashboards."""
    n = len(results)
    disc_n = sum(1 for r in results if r.disclosure_found_exact or r.disclosure_found_normalized)
    vis_n = sum(1 for r in results if r.disclosure_visible)
    payload = {
        "n_page_runs": n,
        "disclosure_found_rate": (disc_n / n) if n else 0.0,
        "disclosure_viewport_visible_rate": (vis_n / n) if n else 0.0,
        "blocked_login_rate": sum(1 for r in results if r.blocked_by_login) / n if n else 0.0,
        "blocked_address_gate_rate": sum(1 for r in results if r.blocked_by_address_gate) / n if n else 0.0,
    }
    write_text_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False))
    log.info("Wrote aggregated summary: %s", path)


def write_aggregated_summary_csv(path: Path, results: list[PageRunResult], log: logging.Logger) -> None:
    """Single-row CSV mirror of aggregated_summary.json for spreadsheet workflows."""
    n = len(results)
    disc_n = sum(1 for r in results if r.disclosure_found_exact or r.disclosure_found_normalized)
    vis_n = sum(1 for r in results if r.disclosure_visible)
    row = {
        "n_page_runs": n,
        "disclosure_found_rate": f"{(disc_n / n) if n else 0.0:.6f}",
        "disclosure_viewport_visible_rate": f"{(vis_n / n) if n else 0.0:.6f}",
        "blocked_login_rate": f"{sum(1 for r in results if r.blocked_by_login) / n if n else 0.0:.6f}",
        "blocked_address_gate_rate": f"{sum(1 for r in results if r.blocked_by_address_gate) / n if n else 0.0:.6f}",
    }
    fields = list(row.keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerow(row)
    log.info("Wrote aggregated summary CSV: %s", path)


CSV_FIELDS = [
    "run_id",
    "timestamp",
    "platform_name",
    "category",
    "homepage_url",
    "target_url",
    "pilot_priority",
    "requires_login",
    "likely_price_page_type",
    "persona_name",
    "persona_group",
    "account_state",
    "url",
    "page_stage",
    "click_depth",
    "path_taken_json",
    "selectors_clicked_json",
    "pricing_state_confidence",
    "likely_price_page_type_detected",
    "price_found",
    "num_price_candidates",
    "top_price_text",
    "fee_candidates_json",
    "disclosure_found_exact",
    "disclosure_found_normalized",
    "disclosure_found_near_match",
    "first_found_stage",
    "first_found_click_depth",
    "first_found_url",
    "clicks_required_to_first_disclosure",
    "disclosure_visible",
    "disclosure_font_size_px",
    "disclosure_font_weight",
    "disclosure_font_size_ratio_vs_body",
    "disclosure_line_height_px",
    "disclosure_contrast_ratio",
    "disclosure_requires_scroll",
    "disclosure_clicks_required",
    "disclosure_position_relative_to_price",
    "disclosure_distance_from_top",
    "disclosure_distance_px_from_price",
    "disclosure_placement_surface",
    "disclosure_alongside_price_visual_group",
    "disclosure_max_z_index",
    "disclosure_visually_obscured",
    "disclosure_visible_without_interaction",
    "disclosure_on_separate_visual_surface",
    "research_timing_first_disclosure",
    "interactions_before_first_disclosure",
    "scroll_depth_to_disclosure",
    "normalized_price_value",
    "currency_code",
    "pricing_base_price_value",
    "pricing_total_price_value",
    "pricing_cart_total_candidate",
    "pricing_fee_items_json",
    "num_disclosure_matches",
    "blocked_by_login",
    "blocked_by_address_gate",
    "used_storage_state",
    "used_test_address",
    "stop_reason",
    "screenshot_path",
    "screenshot_annotated_path",
    "html_path",
    "json_path",
    "error",
]


def result_to_csv_row(r: PageRunResult) -> dict[str, Any]:
    return {
        "run_id": r.run_id,
        "timestamp": r.timestamp_iso,
        "platform_name": r.platform_name,
        "category": r.category,
        "homepage_url": r.homepage_url,
        "target_url": r.target_url,
        "pilot_priority": r.pilot_priority,
        "requires_login": r.requires_login,
        "likely_price_page_type": r.likely_price_page_type,
        "persona_name": r.persona_name,
        "persona_group": r.persona_group,
        "account_state": r.account_state,
        "url": r.url,
        "page_stage": r.page_stage,
        "click_depth": r.click_depth,
        "path_taken_json": json.dumps(r.path_taken, ensure_ascii=False),
        "selectors_clicked_json": json.dumps(r.selectors_clicked, ensure_ascii=False),
        "pricing_state_confidence": r.pricing_state_confidence,
        "likely_price_page_type_detected": r.likely_price_page_type_detected,
        "price_found": r.price_found,
        "num_price_candidates": r.num_price_candidates,
        "top_price_text": r.top_price_text,
        "fee_candidates_json": json.dumps(r.fee_candidates, ensure_ascii=False),
        "disclosure_found_exact": r.disclosure_found_exact,
        "disclosure_found_normalized": r.disclosure_found_normalized,
        "disclosure_found_near_match": r.disclosure_found_near_match,
        "first_found_stage": r.first_found_stage,
        "first_found_click_depth": r.first_found_click_depth,
        "first_found_url": r.first_found_url,
        "clicks_required_to_first_disclosure": r.clicks_required_to_first_disclosure,
        "disclosure_visible": r.disclosure_visible,
        "disclosure_font_size_px": r.disclosure_font_size_px,
        "disclosure_font_weight": r.disclosure_font_weight,
        "disclosure_font_size_ratio_vs_body": r.disclosure_font_size_ratio_vs_body,
        "disclosure_line_height_px": r.disclosure_line_height_px,
        "disclosure_contrast_ratio": r.disclosure_contrast_ratio,
        "disclosure_requires_scroll": r.disclosure_requires_scroll,
        "disclosure_clicks_required": r.disclosure_clicks_required,
        "disclosure_position_relative_to_price": r.disclosure_position_relative_to_price,
        "disclosure_distance_from_top": r.disclosure_distance_from_top,
        "disclosure_distance_px_from_price": r.disclosure_distance_px_from_price,
        "disclosure_placement_surface": r.disclosure_placement_surface,
        "disclosure_alongside_price_visual_group": r.disclosure_alongside_price_visual_group,
        "disclosure_max_z_index": r.disclosure_max_z_index,
        "disclosure_visually_obscured": r.disclosure_visually_obscured,
        "disclosure_visible_without_interaction": r.disclosure_visible_without_interaction,
        "disclosure_on_separate_visual_surface": r.disclosure_on_separate_visual_surface,
        "research_timing_first_disclosure": r.research_timing_first_disclosure,
        "interactions_before_first_disclosure": r.interactions_before_first_disclosure,
        "scroll_depth_to_disclosure": r.scroll_depth_to_disclosure,
        "normalized_price_value": r.normalized_price_value,
        "currency_code": r.currency_code,
        "pricing_base_price_value": r.pricing_base_price_value,
        "pricing_total_price_value": r.pricing_total_price_value,
        "pricing_cart_total_candidate": r.pricing_cart_total_candidate,
        "pricing_fee_items_json": r.pricing_fee_items_json,
        "num_disclosure_matches": r.disclosure_match_count,
        "blocked_by_login": r.blocked_by_login,
        "blocked_by_address_gate": r.blocked_by_address_gate,
        "used_storage_state": r.used_storage_state,
        "used_test_address": r.used_test_address,
        "stop_reason": r.stop_reason,
        "screenshot_path": r.screenshot_path,
        "screenshot_annotated_path": r.screenshot_annotated_path,
        "html_path": r.html_path,
        "json_path": r.json_path,
        "error": r.error,
    }


def write_summary_csv(path: Path, results: list[PageRunResult], log: logging.Logger) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in results:
            w.writerow(result_to_csv_row(r))
    log.debug("Wrote summary CSV %s (%d data rows)", path, len(results))


def validate_summary_csv(
    path: Path, expected_rows: int, headers: list[str], log: logging.Logger
) -> bool:
    if not path.exists():
        log.error("summary.csv validation: file missing: %s", path)
        return False
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.reader(f)
        try:
            file_headers = next(rdr)
        except StopIteration:
            log.error("summary.csv validation: empty file: %s", path)
            return False
        if file_headers != headers:
            log.error(
                "summary.csv validation: header mismatch (%s)",
                path,
            )
            return False
        data_rows = sum(1 for _ in rdr)
        if data_rows != expected_rows:
            log.error(
                "summary.csv validation: row count %d != expected %d (%s)",
                data_rows,
                expected_rows,
                path,
            )
            return False
    return True


async def run_audit(cfg: AuditConfig, log: logging.Logger) -> list[PageRunResult]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    run_dir = ensure_run_dir(cfg.output_dir, run_id)
    csv_path = run_dir / "summary.csv"
    aggregated_csv_path = run_dir / "aggregated_summary.csv"

    tasks: list[tuple[AuditTarget, PersonaConfig]] = [
        (t, p) for t in cfg.targets for p in cfg.personas
    ]
    sem = asyncio.Semaphore(max(1, cfg.max_concurrency))
    results: list[PageRunResult] = []

    try:
        raw_results: list[Any] | None = None
        outer_fatal: BaseException | None = None
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=cfg.headless)
                try:

                    async def one_pair(target: AuditTarget, persona: PersonaConfig) -> PageRunResult:
                        async with sem:
                            context: BrowserContext | None = None
                            try:
                                context = await create_context(browser, persona, cfg)
                                page = await context.new_page()
                                return await audit_page(
                                    page,
                                    target.url,
                                    persona,
                                    run_id,
                                    cfg,
                                    log,
                                    platform=target.platform,
                                )
                            except Exception as e:
                                log.exception(
                                    "Audit task failed url=%s persona=%s",
                                    target.url,
                                    persona.name,
                                )
                                return make_error_result(
                                    run_id=run_id,
                                    url=target.url,
                                    target_url=target.url,
                                    persona=persona,
                                    platform=target.platform,
                                    cfg=cfg,
                                    error_msg=f"{type(e).__name__}: {e}",
                                )
                            finally:
                                if context is not None:
                                    try:
                                        await context.close()
                                    except Exception as close_exc:
                                        log.debug("context.close: %s", close_exc)

                    raw_results = await asyncio.gather(
                        *[one_pair(t, p) for t, p in tasks],
                        return_exceptions=True,
                    )
                finally:
                    try:
                        await browser.close()
                    except Exception as close_exc:
                        log.warning("browser.close failed: %s", close_exc)
        except Exception as e:
            log.exception("Playwright lifecycle failed")
            outer_fatal = e

        if outer_fatal is not None:
            msg = f"{type(outer_fatal).__name__}: {outer_fatal}"
            results = [
                make_error_result(
                    run_id=run_id,
                    url=t.url,
                    target_url=t.url,
                    persona=p,
                    platform=t.platform,
                    cfg=cfg,
                    error_msg=msg,
                )
                for t, p in tasks
            ]
        else:
            assert raw_results is not None
            if len(raw_results) != len(tasks):
                log.error(
                    "gather size mismatch got=%d expected=%d",
                    len(raw_results),
                    len(tasks),
                )
                msg = "internal_error: asyncio.gather length mismatch"
                results = [
                    make_error_result(
                        run_id=run_id,
                        url=t.url,
                        target_url=t.url,
                        persona=p,
                        platform=t.platform,
                        cfg=cfg,
                        error_msg=msg,
                    )
                    for t, p in tasks
                ]
            else:
                norm: list[PageRunResult] = []
                for (t, p), rr in zip(tasks, raw_results):
                    if isinstance(rr, PageRunResult):
                        norm.append(rr)
                    else:
                        em = (
                            f"{type(rr).__name__}: {rr}"
                            if isinstance(rr, BaseException)
                            else str(rr)
                        )
                        log.error("Non-result from audit task: %s", em)
                        norm.append(
                            make_error_result(
                                run_id=run_id,
                                url=t.url,
                                target_url=t.url,
                                persona=p,
                                platform=t.platform,
                                cfg=cfg,
                                error_msg=em,
                            )
                        )
                results = norm

        diff_path = run_dir / "persona_differences.csv"
        try:
            write_persona_differences_csv(diff_path, results, log)
        except Exception as pe:
            log.warning("persona_differences.csv skipped: %s", pe)

        summ_path = run_dir / "aggregated_summary.json"
        try:
            write_aggregated_summary_json(summ_path, results, log)
            write_aggregated_summary_csv(aggregated_csv_path, results, log)
        except Exception as ae:
            log.warning("aggregated summary outputs skipped: %s", ae)

    finally:
        write_summary_csv(csv_path, results, log)
        if not validate_summary_csv(csv_path, len(results), CSV_FIELDS, log):
            log.error(
                "summary.csv validation failed run_id=%s expected_rows=%d path=%s",
                run_id,
                len(results),
                csv_path,
            )
        log.info(
            "summary.csv path=%s page_run_rows=%d",
            csv_path.resolve(),
            len(results),
        )
        if aggregated_csv_path.exists():
            log.info("aggregated_summary.csv path=%s", aggregated_csv_path.resolve())
        else:
            log.info("aggregated_summary.csv not written (check earlier warnings)")
    return results


def default_personas() -> list[PersonaConfig]:
    """Three default study arms: desktop × City Hall, iPhone × LIC, Android × Brooklyn."""
    return [
        PersonaConfig(
            name="desktop_nyc_city_hall_fresh",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            geolocation=GeolocationConfig(40.7127, -74.0040),
            timezone="America/New_York",
            persona_group="web_desktop",
            account_state="fresh",
        ),
        PersonaConfig(
            name="iphone_long_island_city_returning",
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
                "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            locale="en-US",
            geolocation=GeolocationConfig(40.7440, -73.9485),
            timezone="America/New_York",
            persona_group="iphone_mobile",
            account_state="returning",
        ),
        PersonaConfig(
            name="android_brooklyn_high_activity",
            user_agent=(
                "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
            ),
            locale="en-US",
            geolocation=GeolocationConfig(40.6943, -73.9852),
            timezone="America/New_York",
            persona_group="android_mobile",
            account_state="high_activity",
        ),
    ]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Audit visibility of NY algorithmic pricing disclosure (research prototype)."
    )
    p.add_argument(
        "--urls",
        nargs="+",
        help="One or more URLs to audit (manual mode; skips built-in registry).",
    )
    p.add_argument(
        "--url-file",
        type=Path,
        help="File with one URL per line (ignored if --urls is set; manual mode).",
    )
    reg = p.add_mutually_exclusive_group()
    reg.add_argument(
        "--pilot-only",
        action="store_true",
        help="Registry mode: only first-wave pilot platforms (default when using registry).",
    )
    reg.add_argument(
        "--all-enabled",
        action="store_true",
        help="Registry mode: all enabled platforms (not limited to pilot list).",
    )
    p.add_argument(
        "--category",
        action="append",
        metavar="SLUG",
        help=(
            "Registry mode: category slug (repeatable). Examples: "
            "food_delivery, grocery_retail, travel_lodging, ticketing_reservations. "
            "Aliases: food, grocery, travel, ticket."
        ),
    )
    p.add_argument(
        "--platform",
        action="append",
        metavar="NAME",
        help="Registry mode: platform display name (repeatable), case-insensitive (e.g. Uber Lyft).",
    )
    p.add_argument(
        "--include-disabled",
        action="store_true",
        help="Registry mode: include entries with enabled=False (default: only enabled).",
    )
    p.add_argument(
        "--list-registry",
        action="store_true",
        help="Print the built-in platform registry and exit (no browser).",
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
    p.add_argument(
        "--max-depth",
        type=int,
        default=DEFAULT_MAX_EXPLORATION_DEPTH,
        help=f"Max exploration steps (clicks) after initial load (default: {DEFAULT_MAX_EXPLORATION_DEPTH}).",
    )
    p.add_argument(
        "--use-test-address",
        action="store_true",
        help="Attempt to fill a test ZIP in visible postal fields (bounded; no guarantee).",
    )
    p.add_argument(
        "--use-storage-state",
        type=Path,
        default=None,
        help="Playwright storage state JSON (cookies); overrides per-persona path if set.",
    )
    p.add_argument(
        "--continue-after-disclosure",
        action="store_true",
        help="Keep exploring after statutory disclosure is first detected (default: stop).",
    )
    p.add_argument(
        "--personas-file",
        type=Path,
        default=None,
        help="JSON file (array of personas) to use instead of built-in default_personas().",
    )
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def print_registry_listing() -> None:
    """Stdout helper for --list-registry."""
    print("platform_name\tcategory_slug\tpilot_priority\tenabled\thomepage_url")
    for plat in SITE_REGISTRY:
        print(
            f"{plat.platform_name}\t{plat.category_slug}\t{plat.pilot_priority}\t"
            f"{plat.enabled}\t{plat.homepage_url}"
        )


def resolve_audit_targets(ns: argparse.Namespace, log: logging.Logger) -> list[AuditTarget]:
    if ns.list_registry:
        print_registry_listing()
        raise SystemExit(0)

    manual = bool(ns.urls or ns.url_file)
    if manual:
        if ns.category or ns.platform or ns.include_disabled or ns.all_enabled or ns.pilot_only:
            log.warning(
                "Registry filters (--category/--platform/--all-enabled/...) "
                "are ignored when using --urls or --url-file."
            )
        if ns.urls:
            return manual_audit_targets(list(ns.urls))
        lines = ns.url_file.read_text(encoding="utf-8").splitlines()
        urls = [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
        return manual_audit_targets(urls)

    # Registry mode (default when no manual URLs)
    if ns.all_enabled:
        pilot_only = False
    else:
        pilot_only = True

    categories: frozenset[str] | None = None
    if ns.category:
        resolved: set[str] = set()
        for c in ns.category:
            slug = _normalize_category_filter(c)
            if slug is None:
                log.error(
                    "Unknown category %r. Valid slugs: %s",
                    c,
                    ", ".join(sorted(CATEGORY_LABELS)),
                )
                raise SystemExit(2)
            resolved.add(slug)
        categories = frozenset(resolved)

    platform_names: frozenset[str] | None = None
    if ns.platform:
        platform_names = frozenset(x.strip().casefold() for x in ns.platform if x.strip())

    enabled_only = not ns.include_disabled
    targets = expand_registry_to_audit_targets(
        pilot_only=pilot_only,
        enabled_only=enabled_only,
        categories=categories,
        platform_names=platform_names,
    )
    return targets


def main() -> None:
    ns = parse_args()
    logging.basicConfig(
        level=getattr(logging, ns.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("ny_disclosure_audit")

    targets = resolve_audit_targets(ns, log)
    if not targets:
        log.error("No audit targets after applying filters (try --all-enabled or relax filters).")
        raise SystemExit(1)

    cfg = AuditConfig(
        targets=targets,
        output_dir=ns.output_dir.resolve(),
        headless=not ns.headed,
        max_concurrency=ns.max_concurrency,
        capture_screenshot=not ns.no_screenshot,
        nav_timeout_ms=ns.nav_timeout_ms,
        max_exploration_depth=ns.max_depth,
        use_test_address=ns.use_test_address,
        continue_after_disclosure=ns.continue_after_disclosure,
        global_storage_state=ns.use_storage_state.resolve()
        if ns.use_storage_state
        else None,
        personas=(
            load_personas_from_json(ns.personas_file.resolve(), log)
            if ns.personas_file
            else default_personas()
        ),
    )

    asyncio.run(run_audit(cfg, log))


if __name__ == "__main__":
    main()
