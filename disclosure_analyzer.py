#!/usr/bin/env python3
"""
disclosure_analyzer.py — Offline analysis of algorithmic/personalized pricing disclosures.

Methodological limitations (read before interpreting scores):
- Static HTML/CSS snapshots cannot reliably reproduce rendered pixel placement, stacking,
  responsive breakpoints, or dynamic content shown only after interaction.
- Screenshots without manual bounding boxes cannot locate disclosure text deterministically;
  this script does not use OCR unless explicitly enabled (default: off).
- Live browser measurement (getBoundingClientRect, getComputedStyle) would be more accurate
  when pages are accessible; this tool is designed for offline captured evidence.

Scores are provisional research indices only, not legal conclusions.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

# Third-party (install: pip install beautifulsoup4 pandas pillow numpy)
try:
    import numpy as np
except ImportError:
    np = None  # type: ignore

try:
    from bs4 import BeautifulSoup, NavigableString, Tag
except ImportError:
    BeautifulSoup = None  # type: ignore

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore

try:
    from PIL import Image, ImageDraw
except ImportError:
    Image = ImageDraw = None  # type: ignore


# ---------------------------------------------------------------------------
# Disclosure and checkout phrase lists (case-insensitive matching)
# ---------------------------------------------------------------------------

DISCLOSURE_PHRASES: Tuple[str, ...] = (
    "this price was set by an algorithm using your personal data",
    "algorithm using your personal data",
    "new york law requires",
    "ny law requires",
    "personalized incentives",
    "personalized promotions",
    "personal information",
    "delivery address",
    "randomized tests",
)

REQUIRED_SENTENCE_MARKERS: Tuple[str, ...] = (
    "this price was set by an algorithm using your personal data",
    "new york law requires",
    "ny law requires",
)

TOTAL_HINTS: Tuple[str, ...] = (
    "order total",
    "total",
    "estimated total",
    "amount due",
)

SUBTOTAL_HINTS: Tuple[str, ...] = ("subtotal", "items subtotal")

FEE_ROW_KEYWORDS: Tuple[str, ...] = (
    "delivery fee",
    "delivery",
    "service fee",
    "service",
    "regulatory response fee",
    "regulatory",
    "fee",
    "tax",
    "tip",
)

CTA_HINTS: Tuple[str, ...] = (
    "continue",
    "next",
    "place order",
    "got it",
    "got it!",
    "pay now",
    "checkout",
    "submit order",
)

LEGAL_DISCLAIMER_HINTS: Tuple[str, ...] = (
    "terms",
    "privacy",
    "policy",
    "disclaimer",
    "consent",
    "by placing",
    "agree",
)


# ---------------------------------------------------------------------------
# Filename → platform inference
# ---------------------------------------------------------------------------

def infer_platform_from_filename(path: Path) -> str:
    stem = path.stem.lower()
    # Strip common suffixes like _capture, -copy
    stem = re.sub(r"[_-](capture|copy|final|\d+)$", "", stem)
    known = (
        "doordash",
        "instacart",
        "postmates",
        "ubereats",
        "uber_eats",
        "grubhub",
        "seamless",
    )
    for k in known:
        if k in stem.replace("-", "_"):
            return k.replace("_", "")
    return stem or "unknown"


def capture_key_from_path(path: Path) -> str:
    """Key used in annotations.json (basename without extension)."""
    return path.stem.lower()


# ---------------------------------------------------------------------------
# HTML extraction from messy DevTools dumps
# ---------------------------------------------------------------------------

def split_html_and_tail(raw: str) -> Tuple[str, str]:
    """
    Separate HTML fragment from trailing CSS/computed-style paste.
    Heuristic: prefer content through last </html>; else through last closing tag block.
    """
    text = raw.strip()
    if not text:
        return "", ""

    lower = text.lower()
    idx_html_end = lower.rfind("</html>")
    if idx_html_end != -1:
        html_part = text[: idx_html_end + len("</html>")]
        tail = text[idx_html_end + len("</html>") :].strip()
        return html_part, tail

    # Fallback: split when many consecutive lines look like CSS-only (no '<')
    lines = text.splitlines()
    css_start = None
    html_line_count = 0
    for i, line in enumerate(lines):
        s = line.strip()
        if not s:
            continue
        if "<" in s and re.search(r"</?[a-zA-Z]", s):
            html_line_count += 1
            css_start = None
            continue
        # Line looks like CSS property or flattened computed style
        if re.match(r"^[\w-]+\s*:", s) or re.match(
            r"^[a-zA-Z]+\s*rgb\(", s
        ) or re.match(r"^[a-zA-Z]+\d+\.?\d*px", s):
            if css_start is None and html_line_count >= 3:
                css_start = i
                break
    if css_start is not None:
        return "\n".join(lines[:css_start]).strip(), "\n".join(lines[css_start:]).strip()

    return text, ""


def dedupe_repeated_blocks(html: str, min_block_len: int = 200) -> str:
    """Drop obvious duplicate consecutive HTML chunks (common copy-paste error)."""
    if len(html) < min_block_len * 2:
        return html
    n = len(html) // 2
    for half in range(min_block_len, n + 1, 50):
        if html[:half] == html[half : half * 2]:
            return html[:half]
    return html


def parse_soup(html_fragment: str):
    if BeautifulSoup is None:
        raise RuntimeError("beautifulsoup4 is required: pip install beautifulsoup4")
    # Try full document first; fragments are OK with html.parser
    return BeautifulSoup(html_fragment, "html.parser")


# ---------------------------------------------------------------------------
# Text normalization and search
# ---------------------------------------------------------------------------

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def element_visible_text(el: Tag) -> str:
    parts: List[str] = []

    def walk(node):
        if isinstance(node, NavigableString):
            parts.append(str(node))
        elif isinstance(node, Tag):
            if node.name in ("script", "style", "noscript"):
                return
            for c in node.children:
                walk(c)

    walk(el)
    return normalize_ws(" ".join(parts))


def contains_phrase(text: str, phrase: str) -> bool:
    return phrase.lower() in text.lower()


def find_disclosure_element(soup: Tag) -> Tuple[Optional[Tag], Optional[str], Optional[str]]:
    """Return (element, matched phrase, full disclosure-ish text snippet)."""
    best_el: Optional[Tag] = None
    best_phrase: Optional[str] = None
    best_text = ""

    for phrase in DISCLOSURE_PHRASES:
        # Search in strings of elements
        for el in soup.find_all(True):
            if not isinstance(el, Tag):
                continue
            t = element_visible_text(el)
            if len(t) > 8000:
                continue
            if contains_phrase(t, phrase):
                # Prefer smallest containing element
                if best_el is None or len(t) < len(element_visible_text(best_el)):
                    best_el = el
                    best_phrase = phrase
                    best_text = t
        if best_el is not None:
            break

    if best_el is None:
        return None, None, None

    return best_el, best_phrase, best_text


# ---------------------------------------------------------------------------
# DOM traversal helpers
# ---------------------------------------------------------------------------

def all_elements_preorder(root: Tag) -> List[Tag]:
    out: List[Tag] = []

    def walk(node):
        if isinstance(node, Tag):
            out.append(node)
            for c in node.children:
                walk(c)

    walk(root)
    return out


def element_index_map(root: Tag) -> Dict[int, int]:
    """Map id(tag) -> preorder index."""
    els = all_elements_preorder(root)
    return {id(e): i for i, e in enumerate(els)}


def walk_parents(tag: Optional[Tag]) -> List[Tag]:
    chain: List[Tag] = []
    cur = tag
    while cur is not None and isinstance(cur, Tag):
        chain.append(cur)
        cur = cur.parent if isinstance(cur.parent, Tag) else None
    return chain


def is_modal_like(tag: Optional[Tag]) -> bool:
    if tag is None:
        return False
    for p in walk_parents(tag):
        role = (p.get("role") or "").lower()
        am = (p.get("aria-modal") or "").lower()
        cls = " ".join(p.get("class", []) if isinstance(p.get("class"), list) else []).lower()
        if role == "dialog" or am == "true":
            return True
        if "modal" in cls or "dialog" in cls or "overlay" in cls:
            return True
    return False


def modal_title_if_any(disclosure_el: Optional[Tag]) -> Optional[str]:
    if disclosure_el is None:
        return None
    for p in walk_parents(disclosure_el):
        role = (p.get("role") or "").lower()
        if role == "dialog":
            # aria-labelledby / heading inside
            labelled = p.get("aria-labelledby")
            if labelled:
                target = disclosure_el.find_parent("body") or disclosure_el
                le = target.find(id=labelled) if hasattr(target, "find") else None
                if le and isinstance(le, Tag):
                    return element_visible_text(le)[:500]
            h = p.find(["h1", "h2", "h3", "h4"])
            if isinstance(h, Tag):
                return element_visible_text(h)[:500]
    return None


def modal_forced_or_dialog_like(disclosure_el: Optional[Tag]) -> bool:
    if disclosure_el is None:
        return False
    for p in walk_parents(disclosure_el):
        role = (p.get("role") or "").lower()
        am = (p.get("aria-modal") or "").lower()
        if role == "dialog" or am == "true":
            return True
    return False


def find_candidates_by_text(
    soup: Tag,
    hints: Iterable[str],
    max_elements: int = 80,
) -> List[Tag]:
    hits: List[Tag] = []
    hints_l = tuple(h.lower() for h in hints)
    for el in soup.find_all(True):
        if not isinstance(el, Tag):
            continue
        t = element_visible_text(el)
        if len(t) > 400:
            continue
        tl = t.lower()
        if any(h in tl for h in hints_l):
            hits.append(el)
            if len(hits) >= max_elements:
                break
    return hits


def find_total_element(soup: Tag, disclosure_el: Optional[Tag]) -> Optional[Tag]:
    candidates = find_candidates_by_text(soup, TOTAL_HINTS)
    # Prefer elements that mention currency or "total" as whole word
    scored: List[Tuple[int, Tag]] = []
    for c in candidates:
        t = element_visible_text(c).lower()
        score = 0
        if re.search(r"\$|€|£", element_visible_text(c)):
            score += 3
        if re.search(r"\btotal\b", t):
            score += 2
        if "subtotal" in t and "total" not in t:
            score -= 2
        scored.append((score, c))
    scored.sort(key=lambda x: -x[0])
    for s, c in scored:
        if s >= 2:
            return c
    return candidates[0] if candidates else None


REGULATORY_FEE_HINTS: Tuple[str, ...] = ("regulatory response fee", "regulatory response")

SERVICE_FEE_HINTS: Tuple[str, ...] = ("service fee", "service charge")


def find_element_matching_keywords(soup: Tag, keyword_groups: Tuple[str, ...]) -> bool:
    """True if any element's visible text matches a keyword phrase (substring)."""
    kl = tuple(k.lower() for k in keyword_groups)
    for el in soup.find_all(True):
        if not isinstance(el, Tag):
            continue
        t = element_visible_text(el).lower()
        if len(t) > 500:
            continue
        if any(k in t for k in kl):
            return True
    return False


def find_primary_cta(soup: Tag) -> Optional[Tag]:
    """Prefer button / link / role=button with CTA hints."""
    preferred: List[Tag] = []
    for el in soup.find_all(["button", "a", "div", "span"]):
        if not isinstance(el, Tag):
            continue
        role = (el.get("role") or "").lower()
        t = element_visible_text(el)
        if len(t) > 80:
            continue
        tl = t.lower().strip()
        if role == "button" or el.name == "button" or el.name == "a":
            if any(h == tl or h in tl for h in ("continue", "next", "place order", "got it")):
                preferred.append(el)
    if preferred:
        return preferred[0]
    candidates = find_candidates_by_text(soup, CTA_HINTS, max_elements=40)
    return candidates[0] if candidates else None


def compare_dom_order(
    soup: Tag,
    a: Optional[Tag],
    b: Optional[Tag],
) -> Optional[bool]:
    """Return True if a appears after b in preorder; None if unknown."""
    if a is None or b is None:
        return None
    idx = element_index_map(soup)
    ia, ib = idx.get(id(a)), idx.get(id(b))
    if ia is None or ib is None:
        return None
    return ia > ib


def lowest_common_ancestor(a: Optional[Tag], b: Optional[Tag]) -> Optional[Tag]:
    if a is None or b is None:
        return None
    ap = set(walk_parents(a))
    for p in walk_parents(b):
        if p in ap:
            return p
    return None


def same_broad_container(a: Optional[Tag], b: Optional[Tag]) -> bool:
    """Heuristic: LCA is not only html/body and is relatively shallow."""
    lca = lowest_common_ancestor(a, b)
    if lca is None:
        return False
    if lca.name in ("html", "[document]"):
        return False
    depth = 0
    cur: Optional[Tag] = lca
    while cur is not None and cur.name != "body":
        depth += 1
        cur = cur.parent if isinstance(cur.parent, Tag) else None
    return depth <= 12 and lca.name != "body"


def count_fee_rows_before(soup: Tag, disclosure_el: Optional[Tag]) -> int:
    if disclosure_el is None:
        return 0
    idxm = element_index_map(soup)
    di = idxm.get(id(disclosure_el))
    if di is None:
        return 0
    count = 0
    for el in soup.find_all(["tr", "li", "div", "p"]):
        if not isinstance(el, Tag):
            continue
        ei = idxm.get(id(el))
        if ei is None or ei >= di:
            continue
        t = element_visible_text(el).lower()
        if any(k in t for k in FEE_ROW_KEYWORDS) and len(t) < 300:
            if re.search(r"\$|€|£|\d+\.\d{2}", t):
                count += 1
    return count


def count_nearby_interactive(soup: Tag, disclosure_el: Optional[Tag]) -> int:
    """Links/buttons/icons in parent + sibling cluster."""
    if disclosure_el is None:
        return 0
    parent = disclosure_el.parent
    cluster: List[Tag] = [disclosure_el]
    if isinstance(parent, Tag):
        cluster.append(parent)
        for sib in getattr(parent, "children", []):
            if isinstance(sib, Tag):
                cluster.append(sib)
    tags_interactive = {"a", "button"}
    n = 0
    seen: Set[int] = set()
    for root in cluster:
        for el in root.find_all(True):
            if not isinstance(el, Tag):
                continue
            if id(el) in seen:
                continue
            seen.add(id(el))
            name = el.name.lower()
            role = (el.get("role") or "").lower()
            cls = " ".join(el.get("class", []) if isinstance(el.get("class"), list) else []).lower()
            if name in tags_interactive or role in ("button", "link"):
                n += 1
            elif "icon" in cls or el.get("aria-label"):
                n += 1
    return n


def count_disclaimer_paragraphs_before(soup: Tag, disclosure_el: Optional[Tag]) -> Tuple[int, Optional[int]]:
    """
    Count <p> blocks before disclosure that look like legal/disclaimer paragraphs;
    disclosure_paragraph_index = index of <p> containing disclosure among all <p> (0-based).
    """
    paragraphs = [p for p in soup.find_all("p") if isinstance(p, Tag)]
    idxm = element_index_map(soup)
    di = idxm.get(id(disclosure_el)) if disclosure_el else None
    para_index = None
    for i, p in enumerate(paragraphs):
        if disclosure_el and (disclosure_el == p or disclosure_el in p.descendants):
            para_index = i
            break
        if disclosure_el and p in disclosure_el.descendants:
            para_index = i
            break

    disclaimer_before = 0
    if di is not None:
        for p in paragraphs:
            pi = idxm.get(id(p))
            if pi is None or pi >= di:
                continue
            t = element_visible_text(p).lower()
            if len(t) < 40:
                continue
            if any(h in t for h in LEGAL_DISCLAIMER_HINTS) or len(t) > 120:
                disclaimer_before += 1
    return disclaimer_before, para_index


def classify_disclosure_type(
    disclosure_el: Optional[Tag],
    inside_modal: bool,
    near_tooltip: bool,
) -> str:
    if disclosure_el is None:
        return "not_found"
    if near_tooltip:
        return "tooltip_or_info_icon"
    if inside_modal:
        return "modal"
    t = element_visible_text(disclosure_el).lower()
    cls = " ".join(
        disclosure_el.get("class", []) if isinstance(disclosure_el.get("class"), list) else []
    ).lower()
    if "order" in cls or "summary" in cls or "total" in cls:
        return "inline_order_summary"
    if any(h in t for h in LEGAL_DISCLAIMER_HINTS) or "policy" in cls:
        return "inline_legal_disclaimer"
    return "inline_order_summary"


def detect_tooltip_or_info_context(disclosure_el: Optional[Tag]) -> bool:
    if disclosure_el is None:
        return False
    for p in walk_parents(disclosure_el):
        cls = " ".join(p.get("class", []) if isinstance(p.get("class"), list) else []).lower()
        role = (p.get("role") or "").lower()
        if "tooltip" in cls or "popover" in cls:
            return True
        if role == "tooltip":
            return True
        al = (p.get("aria-label") or "").lower()
        if "info" in cls or "info" in al:
            return True
    return False


# ---------------------------------------------------------------------------
# Required sentence analysis
# ---------------------------------------------------------------------------

def find_required_sentence_span(text: str) -> Optional[Tuple[int, int]]:
    tl = text.lower()
    best: Optional[Tuple[int, int]] = None
    for marker in REQUIRED_SENTENCE_MARKERS:
        pos = tl.find(marker.lower())
        if pos != -1:
            end = pos + len(marker)
            if best is None or pos < best[0]:
                best = (pos, end)
    return best


def words_before_required_sentence(text: str) -> Optional[int]:
    span = find_required_sentence_span(text)
    if span is None:
        return None
    prefix = text[: span[0]]
    return len(prefix.split())


def required_sentence_standalone(disclosure_el: Optional[Tag], text: str) -> bool:
    if disclosure_el is None:
        return False
    span = find_required_sentence_span(text)
    if span is None:
        return False
    # Check if the sentence is alone in its paragraph
    for p in disclosure_el.find_all("p"):
        if isinstance(p, Tag) and contains_phrase(element_visible_text(p), REQUIRED_SENTENCE_MARKERS[0][:20]):
            pt = element_visible_text(p)
            return len(pt) < 400 and span[1] - span[0] > 10
    # Single text node dominance
    return len(text) < 350


def required_sentence_bolded(disclosure_el: Optional[Tag], text: str) -> bool:
    if disclosure_el is None:
        return False
    span = find_required_sentence_span(text)
    if span is None:
        return False
    # Walk tags containing strong/bold around substring
    for tag_name in ("strong", "b"):
        for el in disclosure_el.find_all(tag_name):
            if isinstance(el, Tag) and contains_phrase(element_visible_text(el), "algorithm"):
                return True
    # Inline style font-weight on descendants
    for el in disclosure_el.find_all(True):
        if not isinstance(el, Tag):
            continue
        st = el.get("style") or ""
        if "font-weight" in st.lower() and any(
            x in element_visible_text(el).lower() for x in ("algorithm", "law requires")
        ):
            if "700" in st or "bold" in st.lower():
                return True
    return False


# ---------------------------------------------------------------------------
# CSS / computed-style parsing & WCAG contrast
# ---------------------------------------------------------------------------

def parse_rgb(s: str) -> Optional[Tuple[float, float, float]]:
    s = s.strip()
    m = re.search(
        r"rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*([\d.]+))?\s*\)",
        s,
        re.I,
    )
    if m:
        return float(m.group(1)), float(m.group(2)), float(m.group(3))
    m = re.search(r"#([0-9a-fA-F]{6})\b", s)
    if m:
        hx = m.group(1)
        return tuple(int(hx[i : i + 2], 16) for i in (0, 2, 4))  # type: ignore
    return None


def relative_luminance(rgb: Tuple[float, float, float]) -> float:
    def channel(c: float) -> float:
        c = c / 255.0
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = rgb
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)


def contrast_ratio(fg_rgb: Tuple[float, float, float], bg_rgb: Tuple[float, float, float]) -> float:
    l1 = relative_luminance(fg_rgb)
    l2 = relative_luminance(bg_rgb)
    hi, lo = max(l1, l2), min(l1, l2)
    return (hi + 0.05) / (lo + 0.05)


def parse_px(s: str) -> Optional[float]:
    m = re.search(r"([\d.]+)\s*px", s, re.I)
    if m:
        return float(m.group(1))
    return None


def parse_computed_styles_from_text(block: str) -> Dict[str, str]:
    """
    Parse normal CSS lines and flattened DevTools computed-style lines.
    Returns lowercased property keys → raw value strings.
    """
    out: Dict[str, str] = {}
    if not block:
        return out

    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("//"):
            continue

        # Normal: `color: rgb(47, 49, 55);`
        mcol = re.match(r"^([a-zA-Z*-]+)\s*:\s*(.+)$", line)
        if mcol:
            key = mcol.group(1).strip().lower()
            val = mcol.group(2).strip().rstrip(";")
            out[key] = val
            continue

        # Flattened: colorrgb(47, 49, 55)  OR  color rgb(47, 49, 55)
        mflat_rgb = re.match(
            r"^([a-zA-Z-]+)\s*rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", line
        )
        if mflat_rgb:
            prop = mflat_rgb.group(1).lower()
            out[prop] = f"rgb({mflat_rgb.group(2)}, {mflat_rgb.group(3)}, {mflat_rgb.group(4)})"
            continue
        mflat_rgb_nospace = re.match(
            r"^([a-zA-Z-]+)rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", line
        )
        if mflat_rgb_nospace:
            prop = mflat_rgb_nospace.group(1).lower()
            out[prop] = (
                f"rgb({mflat_rgb_nospace.group(2)}, "
                f"{mflat_rgb_nospace.group(3)}, {mflat_rgb_nospace.group(4)})"
            )
            continue

        # Flattened: height485.2px
        mflat_px = re.match(r"^([a-zA-Z]+)(\d+\.?\d*)px$", line)
        if mflat_px:
            prop = mflat_px.group(1).lower()
            out[prop] = f"{mflat_px.group(2)}px"
            continue

        # Flattened catch-all: font-familyUberMoveText...
        mflat_tail = re.match(r"^([a-zA-Z-]+)(.+)$", line)
        if mflat_tail and "rgb(" not in line and not re.search(r"\d+\.?\d*px$", line):
            prop = mflat_tail.group(1).lower()
            if prop not in out:
                out[prop] = mflat_tail.group(2).strip()

    return out


INTERESTING_STYLE_KEYS = (
    "color",
    "background-color",
    "font-size",
    "font-weight",
    "line-height",
    "width",
    "height",
    "display",
    "opacity",
    "visibility",
    "padding",
    "border",
    "box-shadow",
    "border-radius",
    "position",
)


def merge_inline_style(style_attr: str) -> Dict[str, str]:
    parts: Dict[str, str] = {}
    if not style_attr:
        return parts
    for chunk in style_attr.split(";"):
        if ":" in chunk:
            k, v = chunk.split(":", 1)
            parts[k.strip().lower()] = v.strip()
    return parts


def extract_style_maps_for_disclosure(
    disclosure_el: Optional[Tag],
    css_tail: str,
) -> Tuple[Dict[str, str], Dict[str, str], bool]:
    """
    Returns (interesting_styles, full_merged, contrast_background_assumed)
    Preference: inline style on element + ancestors, then tail CSS keys.
    """
    merged: Dict[str, str] = {}
    merged.update(parse_computed_styles_from_text(css_tail))

    if disclosure_el is not None:
        for anc in walk_parents(disclosure_el):
            st = anc.get("style")
            if st:
                merged.update(merge_inline_style(st))

    interesting = {k: merged[k] for k in INTERESTING_STYLE_KEYS if k in merged}

    fg = None
    bg = None
    contrast_background_assumed = True
    if "color" in interesting:
        fg = parse_rgb(interesting["color"])
    if "background-color" in interesting:
        g = parse_rgb(interesting["background-color"])
        if g:
            bg = g
            contrast_background_assumed = False

    if fg is not None and bg is None:
        bg = (255.0, 255.0, 255.0)

    wcag_contrast: Optional[float] = None
    if fg is not None and bg is not None:
        wcag_contrast = contrast_ratio(fg, bg)

    # Pack wcag into interesting dict for export
    out_interesting = dict(interesting)
    if wcag_contrast is not None:
        out_interesting["_wcag_contrast_ratio"] = f"{wcag_contrast:.4f}"
    out_interesting["_contrast_background_assumed"] = str(contrast_background_assumed).lower()

    return out_interesting, merged, contrast_background_assumed


# ---------------------------------------------------------------------------
# Screenshot analysis (no OCR by default)
# ---------------------------------------------------------------------------

def load_image_safe(path: Path):
    if Image is None:
        return None
    try:
        return Image.open(path).convert("RGBA")
    except Exception:
        return None


def luminance_percentiles_crop(img, box: Tuple[int, int, int, int]) -> Optional[Dict[str, float]]:
    """Rough luminance stats inside box; for research only."""
    if np is None or img is None:
        return None
    x, y, w, h = box
    W, H = img.size
    x0, y0 = max(0, x), max(0, y)
    x1, y1 = min(W, x + w), min(H, y + h)
    if x1 <= x0 or y1 <= y0:
        return None
    crop = img.crop((x0, y0, x1, y1))
    arr = np.array(crop.convert("RGB"), dtype=np.float64) / 255.0
    # Rec. 709 luma
    lum = 0.2126 * arr[:, :, 0] + 0.7152 * arr[:, :, 1] + 0.0722 * arr[:, :, 2]
    flat = lum.ravel()
    return {
        "luminance_p10": float(np.percentile(flat, 10)),
        "luminance_p50": float(np.percentile(flat, 50)),
        "luminance_p90": float(np.percentile(flat, 90)),
    }


def draw_annotated_screenshot(
    base_img,
    boxes: Dict[str, List[int]],
    out_path: Path,
) -> None:
    if base_img is None or ImageDraw is None:
        return
    img = base_img.copy().convert("RGBA")
    draw = ImageDraw.Draw(img)
    colors = {
        "disclosure": (255, 0, 0, 255),
        "total": (0, 128, 255, 255),
        "primary_cta": (0, 200, 0, 255),
        "summary_panel": (200, 200, 0, 180),
        "modal": (200, 0, 200, 180),
    }
    for name, rect in boxes.items():
        if len(rect) != 4:
            continue
        color = colors.get(name, (128, 128, 128, 255))
        x, y, w, h = rect
        draw.rectangle([x, y, x + w, y + h], outline=color[:3], width=3)
        draw.text((x + 4, y + 4), name, fill=color[:3])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.convert("RGB").save(out_path)


def rect_contains(inner: Tuple[int, int, int, int], outer: Tuple[int, int, int, int]) -> bool:
    ix, iy, iw, ih = inner
    ox, oy, ow, oh = outer
    return ix >= ox and iy >= oy and ix + iw <= ox + ow and iy + ih <= oy + oh


def euclidean_center_distance(a: Tuple[int, int, int, int], b: Tuple[int, int, int, int]) -> float:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    acx, acy = ax + aw / 2, ay + ah / 2
    bcx, bcy = bx + bw / 2, by + bh / 2
    return math.hypot(acx - bcx, acy - bcy)


def vertical_gap_below_upper(lower: Tuple[int, int, int, int], upper: Tuple[int, int, int, int]) -> float:
    """Positive if lower's top is below upper's bottom."""
    _, _, _, uh = upper
    uy2 = upper[1] + uh
    ly = lower[1]
    return ly - uy2


# ---------------------------------------------------------------------------
# Scoring (editable research indices)
# ---------------------------------------------------------------------------

def score_prominence_0_10(row: Dict[str, Any]) -> float:
    s = 0.0
    cr = row.get("wcag_contrast_ratio")
    try:
        cr_f = float(cr) if cr is not None and str(cr) != "nan" else None
    except (TypeError, ValueError):
        cr_f = None
    if cr_f is not None:
        if cr_f >= 4.5:
            s += 2
        elif cr_f >= 3.0:
            s += 1

    share = row.get("disclosure_area_share_of_viewport")
    try:
        sh = float(share) if share is not None and str(share) != "nan" else None
    except (TypeError, ValueError):
        sh = None
    if sh is not None:
        if sh >= 0.01:
            s += 2
        elif sh >= 0.003:
            s += 1

    if row.get("inside_modal"):
        s += 2

    wbr = row.get("words_before_required_sentence")
    standalone = row.get("required_sentence_standalone")
    if standalone or (isinstance(wbr, (int, float)) and wbr is not None and float(wbr) < 30):
        s += 2

    nd = row.get("number_of_disclaimer_paragraphs_before_disclosure")
    if isinstance(nd, (int, float)) and nd >= 4:
        s -= 1

    return max(0.0, min(10.0, s))


def score_placement_0_10(row: Dict[str, Any]) -> float:
    s = 0.0
    if row.get("disclosure_fully_above_fold"):
        s += 2
    if row.get("same_container_as_total") or row.get("disclosure_inside_summary_panel"):
        s += 2
    da = row.get("disclosure_before_cta_dom")
    db = row.get("disclosure_after_cta_dom")
    if da is True or (da is None and db is not True):
        # prefer before CTA when known
        if da is True:
            s += 2
    if row.get("disclosure_near_total_dom"):
        s += 1
    if row.get("modal_forced_or_dialog_like") or row.get("disclosure_inside_modal_box"):
        s += 2
    return max(0.0, min(10.0, s))


def score_friction_0_10(row: Dict[str, Any]) -> float:
    s = 0.0
    clicks = row.get("clicks_to_visible")
    if clicks == 0:
        s += 3
    elif clicks is None:
        s += 1
    dtype = row.get("disclosure_type") or ""
    if dtype != "tooltip_or_info_icon":
        s += 2
    if row.get("disclosure_above_fold") or row.get("disclosure_fully_above_fold"):
        s += 1
    if row.get("inside_modal") and row.get("modal_forced_or_dialog_like"):
        s += 1
    return max(0.0, min(10.0, s))


def overall_simple_score(p: float, pl: float, f: float) -> float:
    return max(0.0, min(10.0, (p + pl + f) / 3.0))


# ---------------------------------------------------------------------------
# Deterministic notes
# ---------------------------------------------------------------------------

def build_notes_auto(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    if row.get("inside_modal"):
        parts.append("Disclosure found in modal dialog.")
    if row.get("disclosure_after_cta_dom"):
        parts.append("Disclosure appears after primary CTA in DOM.")
    if row.get("disclosure_before_cta_dom"):
        parts.append("Disclosure appears before primary CTA in DOM.")
    nd = row.get("number_of_disclaimer_paragraphs_before_disclosure")
    if isinstance(nd, int) and nd > 0:
        parts.append(f"Disclosure appears after {nd} disclaimer paragraphs.")
    if row.get("disclosure_below_total_screenshot"):
        parts.append("Disclosure appears below total in screenshot annotation.")
    if row.get("disclosure_below_cta_screenshot"):
        parts.append("Disclosure appears below primary CTA in screenshot annotation.")
    if not parts:
        parts.append("Automated summary: see metric columns.")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Main analysis row assembly
# ---------------------------------------------------------------------------

def analyze_capture(
    txt_path: Path,
    _captures_dir: Path,
    annotations: Dict[str, Any],
    enable_ocr: bool,
    write_annotated: bool,
    out_dir: Path,
) -> Dict[str, Any]:
    del enable_ocr  # OCR explicitly off unless CLI enables (placeholder for future)

    raw = txt_path.read_text(encoding="utf-8", errors="replace")
    html_part, tail_css = split_html_and_tail(raw)
    html_part = dedupe_repeated_blocks(html_part)

    soup = parse_soup(html_part)
    root = soup.body if soup.body else soup

    disc_el, matched_phrase, disc_text = find_disclosure_element(root)
    disc_plain = disc_text or ""
    if disc_el is not None:
        disc_plain = element_visible_text(disc_el)

    inside_modal = is_modal_like(disc_el)
    tooltip_like = detect_tooltip_or_info_context(disc_el)
    dtype = classify_disclosure_type(disc_el, inside_modal, tooltip_like)

    total_el = find_total_element(root, disc_el)
    cta_el = find_primary_cta(root)

    preorder_idx = element_index_map(root)

    after_total = compare_dom_order(root, disc_el, total_el)
    after_cta = compare_dom_order(root, disc_el, cta_el)
    before_cta = None
    if after_cta is not None:
        before_cta = not after_cta

    same_total = same_broad_container(disc_el, total_el)
    same_cta = same_broad_container(disc_el, cta_el)

    fee_before = count_fee_rows_before(root, disc_el)
    nearby_ui = count_nearby_interactive(root, disc_el)
    num_disc_p, para_idx = count_disclaimer_paragraphs_before(root, disc_el)

    wbr = words_before_required_sentence(disc_plain)
    req_standalone = required_sentence_standalone(disc_el, disc_plain)
    req_bold = required_sentence_bolded(disc_el, disc_plain)

    interesting, full_styles, bg_assumed = extract_style_maps_for_disclosure(disc_el, tail_css)
    wcag = interesting.get("_wcag_contrast_ratio")
    try:
        wcag_f = float(wcag) if wcag is not None else None
    except (TypeError, ValueError):
        wcag_f = None

    modal_title = modal_title_if_any(disc_el)
    modal_forced = modal_forced_or_dialog_like(disc_el)

    # Near total: immediately after in preorder within small distance
    near_total_dom = False
    if disc_el is not None and total_el is not None:
        ti, di = preorder_idx.get(id(total_el)), preorder_idx.get(id(disc_el))
        if ti is not None and di is not None:
            near_total_dom = 0 < (di - ti) < 25

    row: Dict[str, Any] = {
        "capture_file": str(txt_path.as_posix()),
        "platform_inferred": infer_platform_from_filename(txt_path),
        "disclosure_found_html": disc_el is not None,
        "disclosure_matched_phrase": matched_phrase,
        "disclosure_text": (disc_plain[:2000] if disc_plain else None),
        "disclosure_word_count": len(disc_plain.split()) if disc_plain else 0,
        "disclosure_char_count": len(disc_plain) if disc_plain else 0,
        "disclosure_type": dtype,
        "inside_modal": inside_modal,
        "modal_title": modal_title,
        "modal_forced_or_dialog_like": modal_forced,
        "disclosure_after_total_dom": after_total,
        "disclosure_after_cta_dom": after_cta,
        "disclosure_before_cta_dom": before_cta,
        "total_found_html": total_el is not None,
        "cta_found_html": cta_el is not None,
        "cta_text": element_visible_text(cta_el)[:200] if cta_el else None,
        "number_of_disclaimer_paragraphs_before_disclosure": num_disc_p,
        "disclosure_paragraph_index": para_idx,
        "words_before_required_sentence": wbr,
        "required_sentence_standalone": req_standalone,
        "required_sentence_bolded": req_bold,
        "same_container_as_total": same_total,
        "same_container_as_cta": same_cta,
        "disclosure_near_total_dom": near_total_dom,
        "fee_related_rows_before_disclosure": fee_before,
        "links_buttons_icons_near_disclosure": nearby_ui,
        "wcag_contrast_ratio": wcag_f,
        "contrast_background_assumed": bg_assumed,
        "parsed_styles_interesting_json": json.dumps(
            {k: v for k, v in interesting.items() if not k.startswith("_")},
            ensure_ascii=False,
        ),
        "full_style_keys_count": len(full_styles),
        "style_color": interesting.get("color"),
        "style_background_color": interesting.get("background-color"),
        "style_font_size": interesting.get("font-size"),
        "style_font_weight": interesting.get("font-weight"),
        "style_line_height": interesting.get("line-height"),
        "style_display": interesting.get("display"),
        "style_opacity": interesting.get("opacity"),
        "style_visibility": interesting.get("visibility"),
        "style_position": interesting.get("position"),
        "style_width": interesting.get("width"),
        "style_height": interesting.get("height"),
        "style_padding": interesting.get("padding"),
        "style_border": interesting.get("border"),
        "style_box_shadow": interesting.get("box-shadow"),
        "style_border_radius": interesting.get("border-radius"),
        "subtotal_found_html": find_element_matching_keywords(root, SUBTOTAL_HINTS),
        "delivery_fee_found_html": find_element_matching_keywords(
            root, ("delivery fee", "delivery charge", "delivery charges")
        ),
        "service_fee_found_html": find_element_matching_keywords(root, SERVICE_FEE_HINTS),
        "regulatory_response_fee_found_html": find_element_matching_keywords(
            root, REGULATORY_FEE_HINTS
        ),
        "legal_disclaimer_blocks_found_html": len(
            [p for p in root.find_all("p") if isinstance(p, Tag) and len(element_visible_text(p)) > 80]
        ),
    }

    # Screenshot + annotations
    cap_key = capture_key_from_path(txt_path)
    img_path = txt_path.with_suffix(".png")
    if not img_path.exists():
        for ext in (".jpg", ".jpeg", ".webp"):
            alt = txt_path.with_suffix(ext)
            if alt.exists():
                img_path = alt
                break

    shot_w = shot_h = None
    ann_entry = annotations.get(cap_key) if isinstance(annotations, dict) else None

    img = load_image_safe(img_path) if img_path.exists() else None
    if img is not None:
        shot_w, shot_h = img.size

    # Defaults for screenshot metrics
    null_placements = {
        "screenshot_path": str(img_path.as_posix()) if img_path.exists() else None,
        "screenshot_width": shot_w,
        "screenshot_height": shot_h,
        "disclosure_box_x": None,
        "disclosure_box_y": None,
        "disclosure_box_width": None,
        "disclosure_box_height": None,
        "disclosure_area_px": None,
        "disclosure_area_share_of_viewport": None,
        "disclosure_above_fold": None,
        "disclosure_fully_above_fold": None,
        "disclosure_below_total_screenshot": None,
        "disclosure_below_cta_screenshot": None,
        "disclosure_inside_summary_panel": None,
        "disclosure_inside_modal_box": None,
        "distance_disclosure_to_total_px": None,
        "vertical_gap_disclosure_total_px": None,
        "distance_disclosure_to_cta_px": None,
        "vertical_gap_disclosure_cta_px": None,
        "modal_area_share_of_viewport": None,
        "screenshot_disclosure_luminance_p10": None,
        "screenshot_disclosure_luminance_p50": None,
        "screenshot_disclosure_luminance_p90": None,
        "clicks_to_visible": None,
        "requires_login": None,
        "checkout_stage": None,
        "annotated_screenshot_path": None,
    }
    row.update(null_placements)

    if ann_entry and isinstance(ann_entry, dict):
        boxes = ann_entry.get("boxes") or {}
        manual = ann_entry.get("manual") or {}
        if isinstance(boxes, dict):
            dbox = boxes.get("disclosure")
            tbox = boxes.get("total")
            ctabox = boxes.get("primary_cta")
            sumbox = boxes.get("summary_panel")
            modalbox = boxes.get("modal")

            def as_tuple(b):
                if isinstance(b, (list, tuple)) and len(b) == 4:
                    return int(b[0]), int(b[1]), int(b[2]), int(b[3])
                return None

            dt = as_tuple(dbox)
            tt = as_tuple(tbox)
            ct = as_tuple(ctabox)
            st = as_tuple(sumbox)
            mt = as_tuple(modalbox)

            if dt is not None and shot_w and shot_h:
                dx, dy, dw, dh = dt
                area = max(0, dw) * max(0, dh)
                share = area / float(shot_w * shot_h) if shot_w * shot_h else None
                fold_y = int(shot_h * 0.5) if shot_h else 0
                fully_above = dy >= 0 and dy + dh <= fold_y if shot_h else None

                row.update(
                    {
                        "disclosure_box_x": dx,
                        "disclosure_box_y": dy,
                        "disclosure_box_width": dw,
                        "disclosure_box_height": dh,
                        "disclosure_area_px": area,
                        "disclosure_area_share_of_viewport": share,
                        "disclosure_above_fold": (dy < fold_y) if shot_h else None,
                        "disclosure_fully_above_fold": fully_above,
                    }
                )

                if tt is not None:
                    row["disclosure_below_total_screenshot"] = dt[1] > tt[1] + tt[3] - 1
                    row["distance_disclosure_to_total_px"] = euclidean_center_distance(dt, tt)
                    row["vertical_gap_disclosure_total_px"] = vertical_gap_below_upper(dt, tt)
                if ct is not None:
                    row["disclosure_below_cta_screenshot"] = dt[1] > ct[1] + ct[3] - 1
                    row["distance_disclosure_to_cta_px"] = euclidean_center_distance(dt, ct)
                    row["vertical_gap_disclosure_cta_px"] = vertical_gap_below_upper(dt, ct)
                if st is not None:
                    row["disclosure_inside_summary_panel"] = rect_contains(dt, st)
                if mt is not None:
                    row["disclosure_inside_modal_box"] = rect_contains(dt, mt)
                    if shot_w and shot_h:
                        marea = max(0, mt[2]) * max(0, mt[3])
                        row["modal_area_share_of_viewport"] = marea / float(shot_w * shot_h)

                lum = luminance_percentiles_crop(img, dt) if img is not None else None
                if lum:
                    row["screenshot_disclosure_luminance_p10"] = lum["luminance_p10"]
                    row["screenshot_disclosure_luminance_p50"] = lum["luminance_p50"]
                    row["screenshot_disclosure_luminance_p90"] = lum["luminance_p90"]

            if write_annotated and img is not None and isinstance(boxes, dict):
                annotated_path = out_dir / f"annotated_{cap_key}.png"
                draw_annotated_screenshot(img, boxes, annotated_path)
                row["annotated_screenshot_path"] = str(annotated_path.as_posix())

        if isinstance(manual, dict):
            row["clicks_to_visible"] = manual.get("clicks_to_visible")
            row["requires_login"] = manual.get("requires_login")
            row["checkout_stage"] = manual.get("checkout_stage")

    # Scores
    row["prominence_score_0_to_10"] = score_prominence_0_10(row)
    row["placement_score_0_to_10"] = score_placement_0_10(row)
    row["friction_score_0_to_10"] = score_friction_0_10(row)
    row["overall_simple_score_0_to_10"] = overall_simple_score(
        row["prominence_score_0_to_10"],
        row["placement_score_0_to_10"],
        row["friction_score_0_to_10"],
    )
    row["notes_auto"] = build_notes_auto(row)
    row["disclosure_research_index_note"] = "Provisional research index only; not a legal assessment."

    return row


def load_annotations(path: Optional[Path]) -> Dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze offline HTML/CSS and optional screenshots for pricing disclosures."
    )
    parser.add_argument("--captures", type=Path, default=Path("captures"), help="Directory of .txt captures")
    parser.add_argument("--out", type=Path, default=Path("output"), help="Output directory")
    parser.add_argument(
        "--annotations",
        type=Path,
        default=Path("annotations.json"),
        help="Optional annotations.json (skipped if missing)",
    )
    parser.add_argument(
        "--enable-ocr",
        action="store_true",
        help="Reserved for future OCR-based analysis (disabled in this version).",
    )
    parser.add_argument(
        "--no-annotated-screenshots",
        action="store_true",
        help="Do not write annotated screenshot copies.",
    )
    args = parser.parse_args()

    if pd is None:
        raise RuntimeError("pandas is required: pip install pandas")

    captures_dir = args.captures.resolve()
    out_dir = args.out.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ann_path = args.annotations
    if not ann_path.is_absolute():
        ann_path = Path.cwd() / ann_path
    annotations = load_annotations(ann_path if ann_path.exists() else None)

    txt_files = sorted(captures_dir.glob("*.txt"))
    rows: List[Dict[str, Any]] = []
    for txt in txt_files:
        rows.append(
            analyze_capture(
                txt,
                captures_dir,
                annotations,
                enable_ocr=args.enable_ocr,
                write_annotated=not args.no_annotated_screenshots,
                out_dir=out_dir,
            )
        )

    df = pd.DataFrame(rows)
    csv_path = out_dir / "disclosure_metrics.csv"
    json_path = out_dir / "disclosure_metrics.json"
    df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    df.to_json(json_path, orient="records", indent=2)

    print(f"Wrote {csv_path} ({len(rows)} rows)")
    print(f"Wrote {json_path}")


# ---------------------------------------------------------------------------
# Browser console helper (paste into DevTools on a live checkout page)
# ---------------------------------------------------------------------------

BROWSER_CONSOLE_SNIPPET = r"""
(() => {
  const phrases = [
    "this price was set by an algorithm using your personal data",
    "algorithm using your personal data",
    "new york law requires",
    "ny law requires",
    "personalized incentives",
    "personalized promotions",
  ];
  const totalHints = ["order total", "total", "estimated total", "amount due"];
  const ctaHints = ["continue", "next", "place order", "got it", "pay now"];

  function norm(s) {
    return (s || "").replace(/\s+/g, " ").trim().toLowerCase();
  }

  function walkTextNodes(root) {
    const out = [];
    const tw = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, null);
    let n;
    while ((n = tw.nextNode())) {
      if (n.nodeValue && norm(n.nodeValue).length > 0) out.push(n);
    }
    return out;
  }

  function findByPhrase(phrases) {
    const roots = document.querySelectorAll("body *");
    for (const el of roots) {
      for (const p of phrases) {
        if (norm(el.textContent).includes(p)) {
          let cur = el;
          for (let i = 0; i < 8 && cur; i++) {
            if (cur.textContent && norm(cur.textContent).includes(p)) {
              return { element: cur, phrase: p };
            }
            cur = cur.parentElement;
          }
        }
      }
    }
    return null;
  }

  function findByHints(hints) {
    const cand = Array.from(document.querySelectorAll("button, a, [role='button'], div, span"));
    for (const el of cand) {
      const t = norm(el.textContent);
      if (t.length > 120) continue;
      for (const h of hints) {
        if (t.includes(h)) return el;
      }
    }
    return null;
  }

  function rect(el) {
    if (!el || !el.getBoundingClientRect) return null;
    const r = el.getBoundingClientRect();
    return {
      x: r.x, y: r.y, width: r.width, height: r.height,
      top: r.top, left: r.left, bottom: r.bottom, right: r.right,
    };
  }

  const disclosureHit = findByPhrase(phrases);
  const totalEl = findByHints(totalHints);
  const ctaEl = findByHints(ctaHints);

  const payload = {
    viewport: { width: window.innerWidth, height: window.innerHeight },
    scroll: { scrollX: window.scrollX, scrollY: window.scrollY, scrollHeight: document.documentElement.scrollHeight },
    disclosure: disclosureHit ? { phrase: disclosureHit.phrase, rect: rect(disclosureHit.element), tag: disclosureHit.element.tagName } : null,
    total: totalEl ? { rect: rect(totalEl), textSample: norm(totalEl.textContent).slice(0, 160) } : null,
    cta: ctaEl ? { rect: rect(ctaEl), textSample: norm(ctaEl.textContent).slice(0, 160) } : null,
    computed: {},
  };

  if (disclosureHit && disclosureHit.element) {
    const cs = getComputedStyle(disclosureHit.element);
    [
      "color","background-color","font-size","font-weight","line-height",
      "display","opacity","visibility","padding","border","box-shadow","border-radius","position"
    ].forEach(k => { payload.computed[k] = cs.getPropertyValue(k); });
  }

  const json = JSON.stringify(payload, null, 2);
  try {
    navigator.clipboard.writeText(json);
  } catch (e) {
    console.warn("Clipboard write failed; copying skipped.", e);
  }
  console.log(json);
  return payload;
})();
"""


if __name__ == "__main__":
    main()
