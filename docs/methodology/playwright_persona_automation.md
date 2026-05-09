# Playwright Persona Automation Methodology

## Project purpose

The automation layer (`main.py`) implements a research prototype that audits **visibility and contextual placement** of New York–oriented algorithmic pricing disclosures during scripted browsing sessions. The statutory disclosure text targeted for detection is constant `REQUIRED_DISCLOSURE`: sentence casing `"THIS PRICE WAS SET BY AN ALGORITHM USING YOUR PERSONAL DATA"` (matching logic additionally tolerates normalized variants via JavaScript-side evaluation). The tool measures disclosure detection, DOM-derived typography and proximity cues, optional screenshots, and heuristic pricing-stage classification—it **does not** validate payment correctness or legal compliance.

## What a persona is

A **persona** is a `PersonaConfig` record combining browser-facing signals with metadata used for comparative analysis:

- **Device emulation signals**: named presets (`desktop_browser`, `iphone_mobile`, `android_mobile`) supply distinct **User-Agent** strings and `persona_group`.
- **Locale and clock**: default locale `en-US`, timezone `America/New_York`.
- **Geolocation**: optional `GeolocationConfig` (WGS84 latitude/longitude). When set, Playwright grants geolocation permission and applies coordinates to the browser context.
- **Location labeling**: `location_name` and `location_rationale` tie runs to preset borough-centric presets (`city_hall_manhattan`, `long_island_city_queens`, `brooklyn`) or JSON-supplied equivalents when using `--personas-file`.
- **Account/session stance**: fields such as `account_state` are retained on outputs for research bookkeeping; persistent authentication uses Playwright **storage state** when provided.

Personas can be generated factorially (`default_personas()`) from `--device` and `--location` selections or loaded wholesale from `--personas-file` JSON.

## Why personas matter for personalized pricing research

Personalized or location-sensitive pricing may depend on **device class**, **approximate geography**, and **session history**. Factorial device × location matrices support descriptive comparisons of whether disclosure text appears, remains in viewport, requires clicks or scroll, or co-occurs with price breakdown signals—**holding URL/item constant** while varying environment signals recorded on each `PageRunResult`.

## Browser context setup

For each `(audit target, persona)` pair, `run_audit`:

1. Launches **Chromium** via Playwright (`async_playwright`).
2. Applies optional **`--proxy-server`** at **`chromium.launch`** (default `proxy_scope` is `browser_launch`; context-level proxy is only used when configuration sets `proxy_scope` to `browser_context`, which the stock CLI does not).
3. Opens a **fresh browser context** via `create_context()` with:
   - **Viewport** from `AuditConfig.viewport` (default width 1280, height 800).
   - **User-Agent**, **locale**, **timezone** from the persona.
   - **Geolocation** and **`permissions: ["geolocation"]`** when the persona defines coordinates.
   - **`storage_state`** when `--use-storage-state` points to a readable JSON file **or** when the persona defines `storage_state_path` (global path overrides per-persona paths only when both exist—implementation prefers global file when present).

Concurrency is bounded by `--max-concurrency` (semaphore around each task).

## Viewport, session, cookie, and account handling

| Mechanism | Role |
|-----------|------|
| Viewport | Fixed dimensions on context creation; influences visibility heuristics and screenshots. |
| Cookies / local state | Restored only through Playwright **storage state** JSON (`--use-storage-state` or persona path). |
| Geolocation API | Simulated when persona supplies coordinates; logged as `geolocation_permission_granted`. |
| IP egress | Changes only when a proxy is configured at browser launch; outputs record `ip_proxy_used`, optional `proxy_location` label for alignment metadata. |

There is **no** built-in credential vault or multi-step account onboarding beyond what storage state already encodes.

## How automation navigates platforms

Navigation uses `guided_exploration_loop()`:

1. **`robust_goto`** to the target URL with configurable `--nav-timeout-ms`.
2. **`dismiss_common_overlays`** after a short settle delay.
3. Iterates up to **`max_exploration_depth`** rounds (default 4 **additional** click rounds after initial load; configurable via `--max-depth`).
4. Each round runs **`evaluate_disclosure_all_frames`**: in-page JavaScript scans frames for disclosure phrases, price candidates, gates (login wall, address gate), and a numeric **pricing state score**.
5. Optionally **`--use-test-address`** triggers bounded helpers `_try_fill_test_zip` / `_try_fill_test_address` when the site profile marks address requirements or gates indicate an address gate.

Strategy-dependent `_pick_next_exploration_click` ranks candidate clicks using `SiteProfile.strategy_kind` among `{food_delivery, grocery_retail, travel, ticketing, generic}` when the URL resolves to a registry profile; otherwise **`generic`**.

Stopping rules include: statutory disclosure found (unless `--continue-after-disclosure`), strong pricing score (`≥ 0.65`) after depth > 0, maximum depth, no safe candidates, or navigation failure.

**Important boundary**: the loop **does not submit payment** or complete purchases (explicit docstring).

## Standardized carts and checkouts

The automation **does not** construct identical carts across platforms. “Standardization” is limited to:

- **Registry-defined targets**: URLs/items come from `site_registry.py` expansion or manual `--urls` / `--url-file`.
- **Bounded exploration** toward checkout-adjacent surfaces via heuristic clicks and URL-stage inference (`checkout`, `cart`, etc., from path keywords and JS `likelyPageType`).

Outputs infer checkout proximity from URL paths (e.g., substring checks for `cart`, `checkout`, `pay`) rather than enforcing a common SKU or basket.

## How checkout evidence is captured

After exploration, `audit_page`:

- Writes **full-page HTML** snapshot (`page.content()`).
- Optionally captures **full-page PNG** unless `--no-screenshot`.
- When annotation rectangles exist client-side, may inject overlay highlights and save an **`__annotated`** PNG variant (Playwright-side overlay path in `main.py`).
- Serializes a rich **`PageRunResult`** to **per-row JSON** including disclosure evaluation, price candidates, exploration stages, location alignment metadata, and pricing-breakdown payload derived from merged JS snapshots.

Disclosure matching combines JS-returned candidates with Python-side fields on `PageRunResult`.

## Files saved

Under `output_dir / run_id /`:

| Artifact | Description |
|----------|-------------|
| `{platform_slug}__{url_slug}__{persona}.html` | Raw HTML capture |
| `{platform_slug}__{url_slug}__{persona}.png` | Full-page screenshot (unless disabled) |
| `{platform_slug}__{url_slug}__{persona}__annotated.png` | Optional overlay screenshot |
| `{platform_slug}__{url_slug}__{persona}.json` | Structured run record |

Run-level aggregates:

- **`summary.csv`**: flat table of all `PageRunResult` columns (validated row count).
- **`persona_comparison.csv`**: pairwise factorial summaries where disclosure presence, visibility, clicks-to-see, normalized price, and pricing confidence **differ** across personas sharing platform/item/URL.
- **`aggregated_summary.json`** / **`aggregated_summary.csv`**: run-level counts and rates.

## How results are structured

Each JSON row aligns with `PageRunResult`: platform registry metadata, persona identifiers, location simulation notes (`location_signal_alignment`, `location_simulation_level`), disclosure booleans, typography/contrast fields extracted when candidates exist, scroll and click-depth telemetry, path URLs, fee keyword candidates, and nested **`disclosure`** object (`found_exact`, `found_normalized`, `matched_text`, candidates).

Pricing breakdown fields (`normalized_price_value`, fee lists, etc.) originate from **heuristic DOM/JS parsing**, labeled as research comparison rather than transactional truth inside model docstrings.

## Reproducibility limits

- **Live DOM variance**: A/B layouts, bot defenses, and dynamic pricing change outcomes between dates.
- **Exploration nondeterminism**: Candidate ordering and timing-sensitive overlays can alter click paths within the same bounds.
- **Proxy / IP alignment**: Without controlled egress, server-side personalization signals may diverge from browser geolocation.
- **Storage state drift**: Saved cookies expire; logins break silently until refreshed.
- **Headless vs headed**: `--headed` may shift anti-bot behavior.

Exact rerun parity requires pinning captures externally—the runner alone does not freeze remote HTML.

## Methodological limitations

- Disclosure detection depends on **phrase matching and DOM heuristics**, not regulatory adjudication.
- **Contrast and font metrics** rely on best-effort computed styles in live pages; offline replay uses separate tooling (`disclosure_analyzer.py`).
- Strong **pricing_state_score** early-stop may halt before alternate disclosure placements appear when `--continue-after-disclosure` is off.
- Login walls and captchas may truncate exploration (`blocked_by_login`).
- **found_after_add_to_cart** / **found_after_checkout_transition** flags use **URL substring heuristics**, not verified commerce events.
- **`found_after_login`** is written as **`False`** in the current pipeline regardless of storage-state sessions; it does not encode post-login detection logic.

## Support for policy research

Persona automation yields **systematic, repeatable traces**: who (device persona), where (geo + optional proxy metadata), which URL, how many interactions elapsed before disclosure-related signals appeared, and whether textual disclosure aligned with visible price regions in instrumented sessions. Those traces ground empirical descriptions of **friction**, **modal segregation**, and **cross-persona variance**—inputs for qualitative coding or quantitative dashboards—while explicitly **not** replacing legal analysis or platform-specific enforcement standards.
