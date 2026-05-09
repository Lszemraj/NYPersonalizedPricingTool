# Disclosure Analyzer Methodology

## Project purpose

`disclosure_analyzer.py` performs **offline** analysis of saved HTML/CSS text captures plus optional reviewer-supplied screenshot geometry and metadata. It produces tabular metrics, validation diagnostics, annotated images, and heuristic **research indices** describing disclosure detectability, placement relative to totals and primary calls-to-action, salience proxies, and workflow friction. Module documentation states explicitly: scores are **provisional research indices**, not legal conclusions.

## Input files

### Text captures (`captures/*.txt`)

Each `.txt` file is expected to contain at minimum an HTML fragment or document. The loader **`split_html_and_tail`** separates:

1. **HTML segment**: preferably through the last `</html>`; otherwise heuristic splitting when trailing lines resemble pasted CSS.
2. **Tail**: trailing DevTools-style dumps (computed styles, rules) merged into style parsing.

Optional **`dedupe_repeated_blocks`** reduces duplicated paste artifacts before parsing.

### Images (`captures/*.png`, etc.)

Screenshots are **not** discovered by globbing images alone. Resolution order (`resolve_screenshot_path`):

1. Relative path in annotations JSON `files.screenshot` (see below).
2. Else same basename as the `.txt` beside it with extensions `.png`, `.jpg`, `.jpeg`, `.webp` searched across sensible base directories.

### Annotations JSON (`txts.json`, `annotations.json`, or any path via `--annotations`)

The CLI default path is **`annotations.json`**; the repository also ships **`txts.json`** following the same schema. If the path is missing, analysis proceeds **without** annotation merges.

Expected shape after `normalize_captures_dictionary`:

```json
{
  "captures": {
    "<capture_key>": {
      "files": { "html": "...", "screenshot": "..." },
      "screenshot": { "width_px": ..., "height_px": ..., "fold_y_px": ..., ... },
      "workflow": { ... },
      "boxes": { "disclosure_box": [x1, x2, y1, y2], ... },
      "colors": { "disclosure_text_rgb": [r,g,b], ... },
      "manual_classification": { ... }
    }
  }
}
```

Top-level capture objects that look like entries (`boxes`, `files`, `workflow`, or `screenshot` keys) may be merged into `captures` with logged warnings.

**Capture key** linking: derived from the basename of each `.txt` file (`capture_key_from_path`, lowercased stem). Annotation entries must use matching keys.

## Command-line usage

| Flag | Effect |
|------|--------|
| `--captures DIR` | Directory of `*.txt` captures (default `./captures`). |
| `--out DIR` | Output directory (default `./output`). |
| `--annotations PATH` | Annotation JSON path (default `./annotations.json`; skipped if absent). |
| `--enable-ocr` | **Placeholder only.** The implementation deletes this flag inside `analyze_capture`; **no OCR runs**. |
| `--enable-screenshot-pixel-stats` | When Pillow and NumPy load, computes **luminance percentiles** inside the disclosure box crop (not text recognition). |
| `--no-annotated-screenshots` | Skips writing `annotated_<capture_key>.png`. |

**Runtime dependencies**: pandas is **required** (`main` raises if absent). BeautifulSoup4 is required when parsing HTML. Pillow enables screenshot loading and annotation drawing; without Pillow, image-based metrics degrade (images fail to load). NumPy is needed for luminance statistics.

## Output files

| Output | Content |
|--------|---------|
| `validation_report.json` | Per-capture validation entries: screenshot resolution, dimensions, missing/invalid boxes, warnings, RGB/workflow presence flags. |
| `disclosure_metrics.csv` / `disclosure_metrics.json` | Full metric rows (pandas DataFrame export). |
| `platform_summary.csv` / `platform_summary.json` | **Subset** of columns listed in `PLATFORM_SUMMARY_COLUMNS` (policy-facing dashboard slice). |
| `annotated_<capture_key>.png` | Labeled rectangle overlay **unless** `--no-annotated-screenshots` or image missing. |

## HTML parsing

**Automated HTML-derived metrics** use BeautifulSoup (`html.parser`). The analyzer:

- Locates a disclosure-bearing element via **`find_disclosure_element`** against phrase lists `DISCLOSURE_PHRASES` (case-insensitive substring matching policy-relevant language including NY statutory fragments).
- Locates **total** / **subtotal** style anchors (`TOTAL_HINTS`, `SUBTOTAL_HINTS`), fee keywords, legal paragraphs, and primary CTA candidates (`CTA_HINTS`).
- Builds preorder **`element_index_map`** for DOM-order comparisons.

## Disclosure phrase detection

Matching is **substring-based** on normalized whitespace (`DISCLOSURE_PHRASES`). Separate **`REQUIRED_SENTENCE_MARKERS`** isolate statutory-adjacent clauses for standalone-area and geometry subsets.

## Disclosure type classification

Function **`classify_disclosure_type`** produces **`disclosure_type`** among:

- `not_found`
- `tooltip_or_info_icon` (ancestor heuristic: tooltip/popover roles/classes/info cues)
- `modal` (`is_modal_like`)
- `inline_order_summary` (class hints include order/summary/total **or** default bucket when not modal/tooltip)
- `inline_legal_disclaimer` (visible text or classes resemble legal/policy blocks)

This taxonomy mixes DOM structure and text heuristics; it is **not** a regulatory taxonomy.

## CTA, total, modal, and legal-text detection

**Automated HTML-derived**:

- **Primary CTA**: `find_primary_cta` ranks interactive nodes whose visible text matches `CTA_HINTS`.
- **Total**: `find_total_element` / keyword scans for totals and fee rows.
- **Modal character**: `inside_modal`, `modal_forced_or_dialog_like`, optional **`modal_title_if_any`**.
- **Legal block prevalence**: counts long paragraphs; **`legal_disclaimer_blocks_found_html`** stores count of qualifying `<p>` nodes.

**Screenshot-assisted / manual-rectangle**:

- **`primary_cta_box`**, **`total_price_box`** / **`total_row_box`**, **`modal_box`**, **`legal_disclaimer_block_box`** come from annotation rectangles when supplied.

## DOM-order metrics

**Automated HTML-derived** (`compare_dom_order`, container checks):

- `disclosure_after_total_dom`, `disclosure_after_cta_dom`, `disclosure_before_cta_dom`
- `same_container_as_total`, `same_container_as_cta`
- `disclosure_near_total_dom` (preorder index gap heuristic)
- `fee_related_rows_before_disclosure`, `links_buttons_icons_near_disclosure`
- `number_of_disclaimer_paragraphs_before_disclosure`, `disclosure_paragraph_index`

## CSS and computed-style parsing

The tail segment after HTML feeds **`parse_computed_styles_from_text`**. **`extract_style_maps_for_disclosure`** merges:

1. Parsed tail declarations.
2. Inline `style` attributes walking ancestors of the disclosure element.

Interesting keys (`INTERESTING_STYLE_KEYS`) include color, font-size, opacity, layout, etc.

**WCAG contrast ratio** (`wcag_contrast_ratio`) is computed when foreground (`color`) and background (`background-color` or assumed white) RGB triplets parse successfully—surfaced as **`computed_or_parsed_css`** via `contrast_source` when present.

If CSS contrast is absent but **`colors`** in annotations supplies sampler-derived RGB with manual contrast fields, **`manual_disclosure_contrast_ratio`** can populate **`wcag_contrast_ratio`** with `contrast_source` transitioning to **`manual_rgb`**.

## Manual screenshot annotation

Human reviewers draw axis-aligned rectangles in pixel space. **`SCREENSHOT_BOX_KEYS`** enumerates accepted keys (disclosure, required sentence, totals, CTA, summary panel, legal block, modal). **`disclosure_box`** alone is **required** for full placement geometry (see validation warnings for missing keys).

## Box format `[x1, x2, y1, y2]`

Constant **`BOX_FORMAT = "x1_x2_y1_y2"`**. Each box array must satisfy `x2 > x1` and `y2 > y1`.

## Conversion to internal rectangles

`normalize_box` maps:

- \(x = x_1\)
- \(y = y_1\)
- `width = x2 - x1`
- `height = y2 - y1`

Internal tuples `(x, y, width, height)` drive containment (`rect_contains`), distances, and drawing.

## Screenshot loading

`load_image_safe` opens via Pillow (RGBA). Failures leave **`screenshot_loaded`** false; screenshot-derived metrics become **`None`** or neutral.

Metadata **`screenshot.viewport_*`** from JSON optionally overrides effective viewport width/height for normalized distances.

## Screenshot-derived metrics

**Automated screenshot-derived** (geometry only; **no OCR**):

- Areas and viewport shares for disclosure and required-sentence boxes.
- **`disclosure_below_total_screenshot`**, **`disclosure_below_cta_screenshot`** via vertical gap sign (`vertical_gap_below_upper`).
- **`disclosure_inside_summary_panel`**, **`disclosure_inside_legal_disclaimer_block`**, **`disclosure_inside_modal_box`** via rectangle containment.
- Euclidean center distances and derived **`distance_*_as_viewport_width_pct`** and **`vertical_gap_*_as_viewport_height_pct`** fields.
- Fold proxies: **`fold_y_px`** from annotations or default `0.5 * image_height`; drives **`disclosure_above_fold`**, **`disclosure_fully_above_fold`** (and required-sentence counterparts).

Optional **`--enable-screenshot-pixel-stats`**: **`screenshot_disclosure_luminance_p10/p50/p90`** (**automated RGB-space sampling**, distinct from WCAG contrast).

## Required-sentence geometry metrics

When **`required_sentence_box`** is present, the analyzer mirrors disclosure geometry: areas, fold flags, containment in legal/modal blocks, below-total / below-CTA booleans, **`required_sentence_above_cta_screenshot`** (`ry + rh <= cta_top`), distances, and **`required_sentence_area_share_of_legal_block`**.

## Area ratios

Pairwise ratios (disclosure vs total, required sentence vs total, CTA vs disclosure, legal/modal vs disclosure or required sentence) use **`safe_ratio`** on bounding-box areas—**automated screenshot-derived** where boxes exist.

## Normalized distances

Center-to-center distances divided by annotated viewport width or height yield percentage columns (e.g., **`distance_disclosure_to_total_as_viewport_width_pct`**). These normalize across resolutions but **assume boxes align with the same pixel coordinate system as the screenshot image**.

## Manual RGB and contrast metrics

The **`colors`** object may include sampler RGB triplets. From these the analyzer exports:

- Manual WCAG contrast fields (**`manual_*_contrast_ratio`**).
- **`disclosure_to_total_price_color_distance_rgb`** and analogous Euclidean distances in RGB space (**RGB-derived**, not WCAG).

**`score_legal_burial_0_10`** adds a small bonus when disclosure RGB is distant from **`surrounding_legal_text_rgb`** above **`RGB_DISTANCE_DISTINCT_THRESHOLD`**—still a coarse salience proxy.

## Workflow and friction metrics

**Workflow-derived** columns originate from annotation **`workflow`** (`WORKFLOW_EXPORT_KEYS`): capture stage, clicks to visible disclosure, login/cart/checkout prerequisites, scroll requirement, modal auto-open, tooltip hiding disclosure, fee/terms interactions.

Aliases expose **`clicks_to_visible`** and **`checkout_stage`**. Legacy nested **`manual`** keys partially backfill absent workflow keys.

**`friction_metrics_confidence`** treats workflow presence and numeric **`clicks_to_visible`** as evidence strength.

## Validation report and warnings

Each capture appends a validation dict (`build_capture_validation_report` / `load_screenshot_annotation_context`): missing files, invalid numeric boxes, placeholder zeros, boxes outside image bounds, extreme area fractions (`BOX_TOO_LARGE_FRAC_OF_VIEWPORT`, `BOX_TOO_SMALL_AREA_PX`), missing **`fold_y`**, modal/total/CTA rectangle inconsistencies (`append_dom_visual_validation_warnings`), etc.

Row-level **`validation_warnings`** merges DOM-visual checks with capture validation; **`validation_warning_count`** counts distinct codes.

## Research scores

**Score-derived metrics** (all clamped 0–10; deterministic weighted sums):

| Column | Inputs (simplified) |
|--------|---------------------|
| **`pre_action_visibility_score_0_to_10`** | Screenshot/manual placement vs CTA, DOM-before-CTA, scroll requirement, clicks, fold. |
| **`price_proximity_score_0_to_10`** | Summary containment, normalized distance to total, modal separation heuristics. |
| **`legal_burial_score_0_to_10`** | Legal-block containment, disclaimer paragraph counts, required-sentence standalone/share, RGB distinctness. |
| **`prominence_score_0_to_10`** | WCAG contrast tier, viewport area share, modal bonus, required-sentence compactness, disclaimer-depth penalty. |
| **`placement_score_0_to_10`** | Fold position, summary/total alignment, DOM order vs CTA, modal cues. |
| **`friction_score_0_to_10`** | Clicks to visibility, tooltip penalty, fold/modal friction cues. |
| **`overall_simple_score_0_to_10`** | Arithmetic mean of prominence, placement, friction scores. |

Scores intentionally blend **HTML**, **screenshot**, **manual classification**, **workflow**, **CSS/manual contrast**, and **RGB distance** signals—document provenance when citing a score component.

## Confidence labels

Categorical **`high` / `medium` / `low`**:

- **`html_metrics_confidence`**: parsing success + disclosure phrase hit strength.
- **`placement_metrics_confidence`**: screenshot loaded plus completeness of disclosure/total/CTA rectangles.
- **`contrast_metrics_confidence`**: prioritizes manual contrast ratios over parsed CSS.
- **`friction_metrics_confidence`**: workflow presence and numeric click counts.
- **`overall_evidence_confidence`**: conservative fusion requiring multiple highs for top tier.

Continuous **`visual_metrics_confidence`** and **`placement_geometry_confidence_index`** in `[0,1]` summarize rectangle completeness (implementation in `visual_placement_confidence`).

## Top policy concern classification

**`classify_top_policy_concern`** emits **`top_policy_concern`** plus explanatory text via a **fixed priority stack**, examples:

- `not_found`
- `insufficient_visual_data`
- `below_cta` (screenshot geometry **or** selective **`manual_classification`** booleans when screenshot ambiguous)
- `requires_scroll`
- `modal_separate_from_price`
- `buried_in_legal_block`
- `low_contrast` (uses **`wcag_contrast_ratio`** against **`LOW_CONTRAST_THRESHOLD`**)
- `low_area_share`
- `weak_price_proximity`
- `no_major_issue_detected`

Thresholds (`NEAR_TOTAL_DISTANCE_WIDTH_PCT`, `SMALL_AREA_SHARE_THRESHOLD`, etc.) are **editable constants** at module top—policy interpretations should cite configured values.

## Platform summary fields

`PLATFORM_SUMMARY_COLUMNS` selects a reduced column set including inferred platform (`infer_platform_from_filename`), disclosure detection phrase, type, modal flags, DOM/ch screenshot placement booleans, area and contrast highlights, distance-to-total normalization, core scores, overall confidence, top concern, and validation warning count.

## Methodological limitations

As codified in the module docstring and logic:

- Static captures omit timing, animation, and post-interaction states absent from the paste.
- Without drawn boxes, screenshot placement metrics cannot anchor disclosure pixels (**no OCR path implemented** despite CLI flag).
- Parsed CSS may omit inherited colors; **`contrast_background_assumed`** flags forced backgrounds.
- **`manual_classification`** fields influence **some** scores and concern classification but are **not** uniformly consumed across every metric.
- Filename-based **`platform_inferred`** can mislabel unconventional filenames.

## Policy-relevant interpretation

Use outputs as **structured observational coding**:

- **Automated HTML-derived** metrics answer whether statutory-adjacent language appears in a frozen DOM and its coarse structural relation to totals and CTAs.
- **Screenshot-derived** metrics operationalize **spatial ordering** and share-of-screen proxies **conditional on faithful rectangles**.
- **Manual annotation layers** (boxes, RGB samples, workflow narratives) encode reviewer judgments essential when automation cannot see rendered pixels.
- **Score-derived** composites summarize multi-signal salience and friction for ranking or screening—they remain **configurable heuristics** requiring transparency when cited in policy memoranda.

Treat disagreement between DOM-order signals and screenshot geometry as diagnostic of responsive layout, clipping, or capture mismatch rather than automatic proof of consumer-visible ordering.
