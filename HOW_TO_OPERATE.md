# How to operate the disclosure audit tool

Short guide for running and interpreting `main.py` after the recent refactor.

## What this tool does

- Opens real browser sessions (Chromium via Playwright) under **multiple personas** (different user agents, geolocation, locale, timezone).
- Visits each URL, looks for the statutory disclosure text:

  **`THIS PRICE WAS SET BY AN ALGORITHM USING YOUR PERSONAL DATA`**

  (including **case-insensitive, whitespace-tolerant** matching).
- Records **visibility metrics** (font size, contrast estimate, placement vs. detected price, scroll needed, rough “click friction” estimate).
- Saves **screenshots**, **HTML snapshots**, **per-page JSON**, and a run-level **CSV summary**.

**Research framing:** This supports **compliance / disclosure visibility** analysis. It does **not** prove that personalized pricing occurred or that an algorithm set the price.

---

## One-time setup

1. **Python 3.11+** recommended.
2. Install Playwright and the browser:

   ```bash
   pip install playwright
   python -m playwright install chromium
   ```

3. From the project folder, run `python main.py --help` to confirm the CLI loads.

---

## How to run it

**URLs on the command line:**

```bash
python main.py --urls https://example.com/item --output-dir ./output
```

**URLs from a file** (one URL per line; lines starting with `#` are ignored):

```bash
python main.py --url-file urls.txt --output-dir ./output
```

**Built-in platform registry (default):** If you pass **neither** `--urls` nor `--url-file`, the tool loads the structured registry in **`site_registry.py`** (food delivery, grocery/retail, travel/flights, ticketing). By default it runs **first-wave pilot** platforms only (`--all-enabled` expands to every enabled entry). Each platform can list multiple **target URLs** (e.g. homepage + a generic search path); every URL is visited **once per persona**.

The auditor runs a **bounded, category-aware exploration** (ranked clicks toward cart/checkout/search—never payment submit) and records stages, paths, disclosure matches (including optional “near” keyword matches), pricing-state confidence, and gate hints (login/address). Use `--max-depth` to cap steps, `--use-test-address` to try a test ZIP in visible postal fields, `--use-storage-state` for a saved Playwright cookie file, and `--continue-after-disclosure` to keep exploring after disclosure text is found.

```bash
# Pilot registry only (default registry mode)
python main.py --output-dir ./output

# All enabled platforms in the registry
python main.py --all-enabled --output-dir ./output

# Travel / lodging category only (slug or alias, e.g. travel)
python main.py --category travel_lodging

# Named platforms only (repeatable; case-insensitive)
python main.py --platform Uber --platform Lyft

# Print registry without scraping
python main.py --list-registry
```

Registry filters (`--category`, `--platform`, `--all-enabled`, `--include-disabled`) are **ignored** when you use `--urls` or `--url-file` (a warning is logged).

### Useful options

| Option | Purpose |
|--------|--------|
| `--headed` | Show the browser window (debugging, seeing cookie banners). Default is headless. |
| `--max-concurrency N` | Parallel browser **contexts** (default: 2). Higher = faster but heavier; be respectful to sites. |
| `--no-screenshot` | Skip PNGs; still writes HTML + JSON + CSV. |
| `--nav-timeout-ms` | Page load timeout (default 45000 ms). |
| `--log-level DEBUG` | Verbose logs (e.g. frame evaluation, overlay clicks). |

---

## Where outputs go

Each run creates a **timestamped folder**:

`output/<run_id>/`

Inside you’ll find:

| Artifact | Contents |
|----------|----------|
| `summary.csv` | One row per **URL × persona** with flattened metrics for spreadsheets. |
| `*.json` | Full detail: all disclosure candidates, prices, paths. |
| `*.html` | Page HTML at capture time. |
| `*.png` | Full-page screenshot (unless `--no-screenshot`). |

`run_id` looks like `20250325T120000Z_a1b2c3d4` (UTC time + short id).

---

## Personas

Personas are defined in code (`default_personas()` in `main.py`): **user agent**, **locale**, **timezone**, **geolocation** (NYC-area examples). Each **URL** is visited **once per persona**, in **separate browser contexts** (isolated cookies/storage for that run).

To add or edit personas, adjust `default_personas()` or extend the script to load from a config file (not implemented yet).

Optional: `PersonaConfig.storage_state_path` can point to a Playwright **storage state** JSON file if you need pre-seeded cookies (advanced).

---

## Reading the CSV (high level)

Registry runs include **`platform_name`**, **`category`**, **`homepage_url`**, **`target_url`** (page audited), **`pilot_priority`**, **`requires_login`**, **`likely_price_page_type`**, plus **`url`** (same as `target_url` for compatibility). Full research notes for each platform are in the per-row **JSON** (`platform_notes`).

Key disclosure columns:

- **`disclosure_found_exact` / `disclosure_found_normalized`** — statutory string vs. normalized match.
- **`disclosure_visible`** — whether the matched element intersects the viewport.
- **`disclosure_font_size_px`**, **`disclosure_contrast_ratio`** — styling / readability proxies.
- **`disclosure_requires_scroll`** — disclosure not fully on-screen without scrolling.
- **`disclosure_clicks_required`** — **heuristic** (hidden panels, details, dialogs, etc.); treat as approximate.
- **`disclosure_position_relative_to_price`** — `above_price`, `near_price`, `below_price`, `unknown`, or `separate_page` (e.g. iframe).
- **`num_disclosure_matches`**, **`num_price_candidates`**, **`top_price_text`**, **`error`**.

Use the matching **`.json`** for the full candidate list and per-candidate fields.

---

## Practical tips

- **Debugging a stubborn page:** use `--headed` and optionally `--log-level DEBUG`.
- **Rate and ethics:** add delays between runs if you scrape many URLs; only test pages you’re allowed to access.
- **Failures in `error`:** network timeouts, blocks, or Playwright errors—check the message and retry with `--headed` or a higher `--nav-timeout-ms`.

---

## Getting code help

Run:

```bash
python main.py --help
```

For implementation details (metrics formulas, iframe handling, merge logic), see comments and structure in `main.py`.
