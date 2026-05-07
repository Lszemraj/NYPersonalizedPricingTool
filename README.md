# NYPersonalizedPricingTool

Playwright-based research scraper for collecting evidence about disclosure visibility related to NY Algorithmic Pricing Disclosure Act (§ 349-A).

## Methodology Notes

- **Items per site**: controlled by `--items-per-site N`. Each output row records `item_id`, `item_label`, `item_category`, `item_index`, and `items_per_site_requested`.
- **Factorial device/location design**: default personas are `3 devices x 3 locations = 9` combinations (`desktop_browser`, `iphone_mobile`, `android_mobile` crossed with `city_hall_manhattan`, `long_island_city_queens`, `brooklyn`).
- **Attribution rationale**: this matrix supports device comparisons while holding location constant, and location comparisons while holding device constant.
- **Location choices**:
  - `city_hall_manhattan`: civic/legal center and central NYC baseline.
  - `long_island_city_queens`: Queens mixed residential/commercial outer-borough area.
  - `brooklyn`: outer-borough residential/commercial comparison area.
- **Browser geolocation vs IP location**:
  - Playwright geolocation simulates browser-reported location when geolocation permission is granted.
  - This does **not** automatically change public IP location.
  - IP alignment requires `--proxy-server` (or an external VPN/proxy).
  - Without proxy/VPN, describe runs as **browser-geolocation simulation**.
- **Location validation categories**:
  - `browser_geo_only`: Playwright browser geolocation was set, but no matching ZIP/address or proxy was used.
  - `browser_geo_plus_zip`: Playwright browser geolocation was set and a matching service ZIP/address was entered.
  - `browser_geo_plus_proxy`: Playwright browser geolocation was set and traffic was routed through a proxy labeled as matching the same location.
  - `browser_geo_plus_proxy_plus_zip`: Browser geolocation, service ZIP/address, and proxy metadata all aligned.
  - `mismatch`: At least one location signal conflicted with another.
  - `unknown`: The scraper lacked enough information to validate alignment.
- **Interpretation caution**: observed differences may be caused by device simulation, browser geolocation, IP location, account state, address gates, cookie state, A/B tests, experimentation, or blocking. The tool collects evidence; it does not independently prove personalized pricing.

## Output Layout

Each run writes to:

`output/YYYY-MM-DD_HH-MM-SS_<run_label>/`

If a collision occurs, suffixes `_02`, `_03`, ... are added.

Run folders include:
- `summary.csv` (one row per item/page x persona)
- `aggregated_summary.csv`
- `persona_comparison.csv`
- per-page JSON, HTML, and screenshots when available

## Proxy Scope

Proxy is applied conservatively at browser launch when `--proxy-server` is set (`proxy_scope=browser_launch` by default). This is for methodological IP alignment, not stealth evasion.
Playwright browser geolocation does not automatically change public IP address. Proxy/VPN must be configured separately and should be interpreted as methodological location alignment, not anti-bot evasion.
