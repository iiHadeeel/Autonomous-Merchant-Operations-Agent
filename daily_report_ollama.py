"""
=============================================================================
 Salla Autonomous Merchant Operations Agent
 Daily Report — English Version · Runs 100% Locally
=============================================================================
 File: daily_report_salla_en.py

 Generates a single self-contained HTML report using Salla's official
 brand colours (white + #00b9ae teal) that runs entirely offline.

 INPUTS  (three JSON files from the previous pipeline stages):
   ← catalog_quality_report.json    (from Stage 2.1 — Catalog Analysis)
   ← sentiment_report.json          (from Stage 2.2 — Sentiment Analysis)
   ← pricing_recommendations.json   (from Stage 2.3 — Pricing Engine)

 OUTPUT:
   → daily_report_salla_en.html     

 USAGE:
   python daily_report_salla_en.py
   open daily_report_salla_en.html

 REQUIREMENTS:
   - Python 3.10+
   - Zero external libraries — only Python's standard library
   - Internet only needed for Google Fonts 
=============================================================================
"""

# ── Standard-library 
import json           # read/write JSON pipeline outputs
import datetime       # date/time stamps on the report
from pathlib import Path  # cross-platform file path handling


# =============================================================================
# SECTION 0 — File paths  (edit these to match your folder structure)
# =============================================================================

CATALOG_PATH   = "/Users/hadeel/Desktop/salla/catalog_quality_report.json"
SENTIMENT_PATH = "/Users/hadeel/Desktop/salla/sentiment_report.json"
PRICING_PATH   = "/Users/hadeel/Desktop/salla/pricing_recommendations.json"
OUTPUT_PATH    = "/Users/hadeel/Desktop/salla/daily_report_salla_en.html"

# =============================================================================
# SECTION 1 — JSON loader
# =============================================================================

def load_json(path: str) -> dict:
    """
    Read a JSON file and return it as a Python dictionary.
    Raises a clear FileNotFoundError if the file is missing so the
    developer knows exactly which pipeline stage needs to be re-run.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"File not found: {path}\n"
            f"Make sure all previous pipeline stages have run (2.1 → 2.2 → 2.3)"
        )
    with open(p, encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# SECTION 2 — Salla brand colour palette 
# =============================================================================
#
# Colours 
#   Primary teal  : #00b9ae  
#   Background    : #f7f8fa  
#   Surface/cards : #ffffff
#   Dark text     : #1a2332 
#   Borders       : #e4e8ed
#   Danger red    : #ef4444
#   Warning amber : #f59e0b
#   Success green : #10b981
#
SALLA_CSS = """
  /* ── Salla official brand tokens ── */
  :root {
    /* Primary teal — used for CTAs, active states, highlights */
    --salla-teal:        #00b9ae;
    --salla-teal-dark:   #009990;   /* hover / pressed state */
    --salla-teal-light:  #e6f7f6;   /* tinted card backgrounds */
    --salla-teal-mid:    #b3e8e6;   /* teal borders */

    /* Page & surface colours */
    --bg:        #f7f8fa;   /* main page background */
    --surface:   #ffffff;   /* card / panel background */
    --surface2:  #f0f4f7;   /* secondary surface (table headers etc.) */
    --border:    #e4e8ed;   /* standard border */
    --border-dk: #d0d6de;   /* slightly darker border */

    /* Typography */
    --text:      #1a2332;   /* primary body text */
    --text-sub:  #4a5568;   /* secondary / label text */
    --text-muted:#8a94a6;   /* placeholder / disabled text */

    /* Status colours */
    --red:          #ef4444;
    --red-bg:       #fef2f2;
    --red-border:   #fecaca;

    --yellow:       #f59e0b;
    --yellow-bg:    #fffbeb;
    --yellow-border:#fde68a;

    --green:        #10b981;
    --green-bg:     #ecfdf5;
    --green-border: #a7f3d0;

    --blue:         #3b82f6;
    --blue-bg:      #eff6ff;
    --blue-border:  #bfdbfe;

    /* Shadows */
    --shadow-sm:   0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
    --shadow-md:   0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
    --shadow-teal: 0 4px 14px rgba(0,185,174,0.20);
  }
"""


# =============================================================================
# SECTION 3 — Full CSS
# =============================================================================

FULL_CSS = SALLA_CSS + """
  /* ── Reset ── */
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    /* DM Sans is a clean, modern LTR-optimised sans-serif */
    font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    direction: ltr;
    text-align: left;
    line-height: 1.5;
  }

  /* ─────────────────────────────────────────
     STICKY HEADER
  ───────────────────────────────────────── */
  .header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0 48px;
    height: 64px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: var(--shadow-sm);
  }

  .header-brand { display: flex; align-items: center; gap: 12px; }

  /* Salla logo square */
  .logo-box {
    width: 38px; height: 38px;
    background: var(--salla-teal);
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    box-shadow: var(--shadow-teal);
    flex-shrink: 0;
  }
  .logo-box svg { width: 22px; height: 22px; fill: white; }

  .brand-text .name  { font-size: 15px; font-weight: 700; color: var(--text); line-height: 1.2; }
  .brand-text .sub   { font-size: 11px; color: var(--text-muted); font-weight: 400; }

  .header-date { text-align: right; }
  .header-date .date { font-size: 20px; font-weight: 700; color: var(--text); }
  .header-date .time { font-size: 11px; color: var(--text-muted); margin-top: 2px; }

  /* ─────────────────────────────────────────
     STICKY SECTION NAVIGATION BAR
  ───────────────────────────────────────── */
  .nav-bar {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0 48px;
    display: flex;
    gap: 4px;
    overflow-x: auto;
    /* hide scrollbar on desktop */
    scrollbar-width: none;
  }
  .nav-bar::-webkit-scrollbar { display: none; }

  .nav-pill {
    padding: 14px 18px;
    font-size: 13px; font-weight: 600;
    color: var(--text-muted);
    border-bottom: 2px solid transparent;
    white-space: nowrap;
    text-decoration: none;
    display: block;
    transition: color 0.2s, border-color 0.2s;
    cursor: pointer;
  }
  .nav-pill:hover { color: var(--salla-teal); }
  .nav-pill.active { color: var(--salla-teal); border-bottom-color: var(--salla-teal); }

  /* ─────────────────────────────────────────
     MAIN CONTENT AREA
  ───────────────────────────────────────── */
  .main { padding: 32px 48px; max-width: 1440px; margin: 0 auto; }

  /* ─────────────────────────────────────────
     CRITICAL ALERT BANNER
     Only rendered when high-severity alerts exist
  ───────────────────────────────────────── */
  .critical-banner {
    background: var(--red-bg);
    border: 1.5px solid var(--red-border);
    border-left: 4px solid var(--red);   /* LTR accent stripe */
    border-radius: 12px;
    padding: 16px 20px;
    margin-bottom: 28px;
    display: flex;
    gap: 14px;
    align-items: flex-start;
    animation: pulse 2.4s ease-in-out infinite;
  }
  @keyframes pulse {
    0%,100% { border-left-color: var(--red); }
    50%      { border-left-color: #fca5a5; }
  }
  .critical-banner .icon  { font-size: 20px; flex-shrink: 0; margin-top: 1px; }
  .critical-banner .title {
    font-size: 12px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.07em; color: var(--red); margin-bottom: 6px;
  }
  .critical-banner .body  { font-size: 13px; color: #7f1d1d; line-height: 1.6; }
  .critical-banner .items { margin-top: 8px; }
  .critical-banner .item  {
    font-size: 12px; color: #991b1b; padding: 3px 0;
    display: flex; align-items: center; gap: 6px;
  }
  .critical-banner .item::before { content: '→'; color: var(--red); }

  /* ─────────────────────────────────────────
     KPI CARD ROW
  ───────────────────────────────────────── */
  .kpi-row {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 16px;
    margin-bottom: 32px;
  }

  .kpi-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 20px;
    box-shadow: var(--shadow-sm);
    transition: box-shadow 0.2s, transform 0.2s;
    position: relative;
    overflow: hidden;
  }
  .kpi-card:hover { box-shadow: var(--shadow-md); transform: translateY(-1px); }

  /* Coloured left accent stripe (LTR) */
  .kpi-card::before {
    content: ''; position: absolute;
    left: 0; top: 0; bottom: 0; width: 3px;
    border-radius: 14px 0 0 14px;
  }
  .kpi-card.teal::before   { background: var(--salla-teal); }
  .kpi-card.red::before    { background: var(--red); }
  .kpi-card.yellow::before { background: var(--yellow); }
  .kpi-card.green::before  { background: var(--green); }
  .kpi-card.blue::before   { background: var(--blue); }

  .kpi-icon {
    width: 40px; height: 40px; border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 18px; margin-bottom: 14px;
  }
  .kpi-card.teal   .kpi-icon { background: var(--salla-teal-light); }
  .kpi-card.red    .kpi-icon { background: var(--red-bg); }
  .kpi-card.yellow .kpi-icon { background: var(--yellow-bg); }
  .kpi-card.green  .kpi-icon { background: var(--green-bg); }
  .kpi-card.blue   .kpi-icon { background: var(--blue-bg); }

  .kpi-label {
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.09em; color: var(--text-muted); margin-bottom: 8px;
  }
  .kpi-value {
    font-size: 30px; font-weight: 800; line-height: 1;
    color: var(--text); margin-bottom: 6px;
  }
  .kpi-card.red    .kpi-value { color: var(--red); }
  .kpi-card.yellow .kpi-value { color: var(--yellow); }
  .kpi-card.green  .kpi-value { color: var(--green); }
  .kpi-card.teal   .kpi-value { color: var(--salla-teal); }
  .kpi-sub { font-size: 11px; color: var(--text-muted); }

  /* ─────────────────────────────────────────
     SECTION LAYOUT
  ───────────────────────────────────────── */
  .section { margin-bottom: 40px; }

  .section-header {
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 18px; padding-bottom: 14px;
    border-bottom: 1px solid var(--border);
  }
  .section-icon {
    width: 36px; height: 36px;
    background: var(--salla-teal-light);
    border-radius: 9px;
    display: flex; align-items: center; justify-content: center;
    font-size: 16px; flex-shrink: 0;
  }
  .section-title { font-size: 17px; font-weight: 700; color: var(--text); }
  .section-badge {
    margin-left: auto;   /* push badge to the right (LTR) */
    font-size: 11px; font-weight: 600;
    padding: 4px 12px; border-radius: 20px; border: 1px solid;
  }

  /* Badge colour variants */
  .badge-red    { background: var(--red-bg);          color: var(--red);          border-color: var(--red-border); }
  .badge-yellow { background: var(--yellow-bg);        color: var(--yellow);       border-color: var(--yellow-border); }
  .badge-green  { background: var(--green-bg);         color: var(--green);        border-color: var(--green-border); }
  .badge-teal   { background: var(--salla-teal-light); color: var(--salla-teal);   border-color: var(--salla-teal-mid); }
  .badge-grey   { background: var(--surface2);         color: var(--text-muted);   border-color: var(--border); }

  /* ─────────────────────────────────────────
     ALERT CARDS GRID
  ───────────────────────────────────────── */
  .alerts-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 14px;
  }
  .alert-card { border-radius: 12px; padding: 18px 20px; border: 1px solid; }
  .alert-card.high   {
    background: var(--red-bg); border-color: var(--red-border);
    border-left: 3px solid var(--red);   /* LTR accent */
  }
  .alert-card.medium {
    background: var(--yellow-bg); border-color: var(--yellow-border);
    border-left: 3px solid var(--yellow);
  }
  .alert-card-head {
    display: flex; align-items: center;
    justify-content: space-between; margin-bottom: 10px;
  }
  .alert-type {
    font-size: 10px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.1em;
  }
  .high   .alert-type { color: var(--red); }
  .medium .alert-type { color: #b45309; }
  .alert-sev { font-size: 10px; font-weight: 700; padding: 2px 8px; border-radius: 4px; }
  .high   .alert-sev { background: #fee2e2; color: var(--red); }
  .medium .alert-sev { background: #fef3c7; color: #92400e; }
  .alert-msg { font-size: 13px; line-height: 1.6; }
  .high   .alert-msg { color: #7f1d1d; }
  .medium .alert-msg { color: #78350f; }
  .alert-meta {
    margin-top: 10px; padding-top: 10px;
    border-top: 1px solid rgba(0,0,0,0.06);
    display: flex; flex-wrap: wrap; gap: 10px; font-size: 11px;
  }
  .high   .alert-meta { color: #b91c1c; }
  .medium .alert-meta { color: #b45309; }

  /* ─────────────────────────────────────────
     HIGH-URGENCY MESSAGES LIST
  ───────────────────────────────────────── */
  .urgency-list { display: flex; flex-direction: column; gap: 10px; }
  .urgency-item {
    background: var(--surface); border: 1px solid var(--border);
    border-left: 3px solid var(--red);   /* LTR accent */
    border-radius: 10px; padding: 14px 18px;
    display: flex; align-items: flex-start; gap: 14px;
    box-shadow: var(--shadow-sm);
  }
  .urgency-badge {
    background: var(--red); color: white;
    font-size: 9px; font-weight: 800; letter-spacing: 0.1em;
    padding: 3px 8px; border-radius: 4px;
    flex-shrink: 0; margin-top: 2px;
    text-transform: uppercase;
  }
  .urgency-summary { font-size: 13px; font-weight: 600; color: var(--text); margin-bottom: 5px; }
  .urgency-meta    { font-size: 11px; color: var(--text-muted); display: flex; flex-wrap: wrap; gap: 12px; }

  /* ─────────────────────────────────────────
     SENTIMENT BAR CHART
  ───────────────────────────────────────── */
  .sentiment-container {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 14px; padding: 24px 28px; margin-bottom: 20px;
    box-shadow: var(--shadow-sm);
  }
  .sentiment-row   { display: flex; align-items: center; gap: 14px; margin-bottom: 16px; }
  .sentiment-row:last-child { margin-bottom: 0; }
  .sentiment-label { font-size: 13px; font-weight: 600; color: var(--text-sub); width: 75px; }
  .bar-wrap  { flex: 1; background: var(--surface2); border-radius: 6px; height: 10px; overflow: hidden; }
  .bar-fill  { height: 100%; border-radius: 6px; transition: width 1s ease; }
  .bar-positive { background: var(--green); }
  .bar-neutral  { background: var(--salla-teal); }
  .bar-negative { background: var(--red); }
  .bar-pct { font-size: 13px; font-weight: 700; width: 38px; }

  /* ─────────────────────────────────────────
     DATA TABLE (sentiment categories)
  ───────────────────────────────────────── */
  .data-table-wrap {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 14px; overflow: hidden; box-shadow: var(--shadow-sm);
  }
  .data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .data-table th {
    font-size: 11px; font-weight: 600; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.08em;
    padding: 10px 16px; text-align: left;
    background: var(--surface2); border-bottom: 1px solid var(--border);
  }
  .data-table td   { padding: 14px 16px; border-bottom: 1px solid var(--border); vertical-align: middle; }
  .data-table tr:last-child td  { border-bottom: none; }
  .data-table tr:hover td { background: var(--salla-teal-light); }

  /* ─────────────────────────────────────────
     PILLS (coloured inline labels)
  ───────────────────────────────────────── */
  .pill {
    display: inline-flex; align-items: center;
    padding: 4px 12px; border-radius: 20px;
    font-size: 11px; font-weight: 600;
    border: 1px solid; white-space: nowrap;
  }
  .pill-teal   { background: var(--salla-teal-light); color: var(--salla-teal);  border-color: var(--salla-teal-mid); }
  .pill-red    { background: var(--red-bg);    color: var(--red);    border-color: var(--red-border); }
  .pill-yellow { background: var(--yellow-bg); color: #b45309;       border-color: var(--yellow-border); }
  .pill-green  { background: var(--green-bg);  color: var(--green);  border-color: var(--green-border); }
  .pill-blue   { background: var(--blue-bg);   color: var(--blue);   border-color: var(--blue-border); }
  .pill-grey   { background: var(--surface2);  color: var(--text-muted); border-color: var(--border); }

  /* ─────────────────────────────────────────
     PRICING RECOMMENDATION CARDS
  ───────────────────────────────────────── */
  .pricing-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 16px;
  }
  .price-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 14px; padding: 20px;
    box-shadow: var(--shadow-sm); transition: box-shadow 0.2s;
    overflow: hidden;
  }
  .price-card:hover { box-shadow: var(--shadow-md); }

  /* Card background tints per recommendation type */
  .price-card.INCREASE {
    border-color: var(--green-border);
    background: linear-gradient(135deg, #fff 0%, var(--green-bg) 100%);
  }
  .price-card.BLOCKED {
    border-color: var(--red-border);
    background: linear-gradient(135deg, #fff 0%, var(--red-bg) 100%);
  }
  .price-card.DATA_INCOMPLETE {
    border-color: var(--yellow-border);
    background: linear-gradient(135deg, #fff 0%, var(--yellow-bg) 100%);
  }

  .pc-head { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 16px; }
  .pc-name { font-size: 15px; font-weight: 700; color: var(--text); }
  .pc-id   { font-size: 10px; color: var(--text-muted); margin-top: 3px; font-family: 'Courier New', monospace; }

  /* Price display strip */
  .pc-prices {
    display: flex; align-items: center; gap: 12px;
    padding: 14px 0;
    border-top: 1px dashed var(--border);
    border-bottom: 1px dashed var(--border);
    margin-bottom: 14px;
  }
  .pc-price-block { text-align: center; flex: 1; }
  .pc-price-lbl {
    font-size: 10px; font-weight: 600; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 4px;
  }
  .pc-price-num  { font-size: 22px; font-weight: 800; }
  .pc-arrow      { font-size: 22px; color: var(--salla-teal); }
  .pc-explanation { font-size: 12px; line-height: 1.7; color: var(--text-sub); }

  /* ─────────────────────────────────────────
     CATALOG HEALTH — ISSUE GRID
  ───────────────────────────────────────── */
  .issue-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 10px;
  }
  .issue-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px 16px;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: var(--shadow-sm); transition: border-color 0.2s;
  }
  .issue-card:hover { border-color: var(--salla-teal-mid); }
  .issue-name  { font-size: 11px; color: var(--text-sub); font-weight: 500; }
  .issue-count { font-size: 20px; font-weight: 800; color: var(--text); }

  /* ─────────────────────────────────────────
     EMPTY STATE (used when a section has no data)
  ───────────────────────────────────────── */
  .empty-state { text-align: center; padding: 40px 20px; color: var(--text-muted); }
  .empty-state .icon { font-size: 36px; display: block; margin-bottom: 10px; }

  /* ─────────────────────────────────────────
     FOOTER
  ───────────────────────────────────────── */
  .footer {
    background: var(--surface); border-top: 1px solid var(--border);
    padding: 20px 48px;
    display: flex; align-items: center; justify-content: space-between;
    font-size: 12px; color: var(--text-muted);
  }

  /* ─────────────────────────────────────────
     RESPONSIVE BREAKPOINTS
  ───────────────────────────────────────── */
  @media (max-width: 900px) {
    .header, .nav-bar, .main, .footer { padding-left: 20px; padding-right: 20px; }
    .kpi-row { grid-template-columns: repeat(2, 1fr); }
  }
  @media (max-width: 600px) {
    .kpi-row { grid-template-columns: 1fr 1fr; }
    .pricing-grid, .alerts-grid { grid-template-columns: 1fr; }
  }
"""


# =============================================================================
# SECTION 4 — HTML content builders 
# =============================================================================

def build_critical_banner(alerts: list) -> str:
    """
    Renders the red pulsing banner at the very top of the page.
    Only shown when at least one alert has severity == 'high'.
    Returns an empty string (no banner) if all alerts are medium/low.
    """
    critical = [a for a in alerts if a.get("severity") == "high"]
    if not critical:
        return ""  # no banner needed

    # List up to 4 alert messages inside the banner
    items_html = "".join(
        f'<div class="item">{a["message"]}</div>'
        for a in critical[:4]
    )

    return f"""
    <div class="critical-banner">
      <div class="icon"></div>
      <div>
        <div class="title">Immediate Action Required — {len(critical)} critical alert(s)</div>
        <div class="body">Review the following before opening your store today</div>
        <div class="items">{items_html}</div>
      </div>
    </div>"""


def build_kpi_row(sentiment: dict, catalog: dict, pricing: dict) -> str:
    """
    Renders the five at-a-glance KPI cards at the top of the report.
    Card colour is determined dynamically by the value:
      - red    = needs urgent attention
      - yellow = worth monitoring
      - green  = healthy
      - teal   = informational
    """
    # Pull data from sentiment executive summary
    es            = sentiment.get("executive_summary", {})
    total_msgs    = es.get("total_messages", 0)
    alert_count   = len(sentiment.get("alerts", []))
    # Count only actionable categories for the KPI card
    hi_urg_count  = sum(
        1 for m in sentiment.get("high_urgency_messages", [])
        if m.get("category") in {"Complaint", "Transactional Request"}
    )
    complaint_cnt = sentiment.get("category_breakdown", {}).get("Complaint", {}).get("count", 0)

    # Catalog: count distinct issue types
    issue_types   = len(catalog.get("summary", {}).get("issue_type_counts", {}))

    # Pricing: count recommendations that require a decision
    recs          = pricing.get("recommendations", [])
    pricing_acts  = sum(1 for r in recs
                        if r["action"] in ("INCREASE", "BLOCKED", "DATA_INCOMPLETE"))

    # Colour thresholds
    alert_cls    = "red"    if alert_count >= 3  else ("yellow" if alert_count > 0 else "green")
    complaint_cls= "red"    if complaint_cnt >= 5 else ("yellow" if complaint_cnt >= 2 else "teal")
    pricing_cls  = "yellow" if pricing_acts > 0   else "green"

    cards = [
        # Card 1 — Active alerts
        f"""
        <div class="kpi-card {alert_cls}">
          <div class="kpi-icon">🚨</div>
          <div class="kpi-label">Active Alerts</div>
          <div class="kpi-value">{alert_count}</div>
          <div class="kpi-sub">{"Needs review" if alert_count > 0 else "All clear ✓"}</div>
        </div>""",

        # Card 2 — Complaints today
        f"""
        <div class="kpi-card {complaint_cls}">
          <div class="kpi-icon">💬</div>
          <div class="kpi-label">Complaints Today</div>
          <div class="kpi-value">{complaint_cnt}</div>
          <div class="kpi-sub">of {total_msgs} total messages</div>
        </div>""",

        # Card 3 — High-urgency messages
        f"""
        <div class="kpi-card {'red' if hi_urg_count > 0 else 'teal'}">
          <div class="kpi-icon">⚡</div>
          <div class="kpi-label">Urgent Messages</div>
          <div class="kpi-value">{hi_urg_count}</div>
          <div class="kpi-sub">{"Require immediate reply" if hi_urg_count > 0 else "Nothing urgent"}</div>
        </div>""",

        # Card 4 — Catalog issue types
        f"""
        <div class="kpi-card yellow">
          <div class="kpi-icon">📦</div>
          <div class="kpi-label">Catalog Issues</div>
          <div class="kpi-value">{issue_types}</div>
          <div class="kpi-sub">distinct issue types found</div>
        </div>""",

        # Card 5 — Pricing decisions pending
        f"""
        <div class="kpi-card {pricing_cls}">
          <div class="kpi-icon">💰</div>
          <div class="kpi-label">Pricing Actions</div>
          <div class="kpi-value">{pricing_acts}</div>
          <div class="kpi-sub">products need a decision</div>
        </div>""",
    ]

    return f'<div class="kpi-row">{"".join(cards)}</div>'


def build_alerts_section(alerts: list) -> str:
    """
    Renders the anomaly alert cards grid.
    Each card shows: alert type · severity badge · message · metadata chips.
    High-severity cards are red; medium are amber.
    """
    if not alerts:
        return """
        <div class="empty-state">
          <span class="icon">✅</span>
          <div>No alerts today — all indicators are normal.</div>
        </div>"""

    # Human readable labels for alert types coming from the pipeline
    type_labels = {
        "COMPLAINT_SPIKE":        "Complaint Spike",
        "HIGH_URGENCY_CLUSTER":   "Urgency Cluster",
        "PRODUCT_QUALITY_ALERT":  "Product Quality Alert",
        "PRODUCT_SENTIMENT_ALERT":"Sentiment Alert",
    }

    cards = []
    for a in alerts:
        sev       = a.get("severity", "medium")
        sev_label = "CRITICAL" if sev == "high" else "MEDIUM"
        type_str  = type_labels.get(a.get("type", ""), a.get("type", ""))

        # Build metadata chips (window, rate, product etc.)
        meta_parts = []
        if "window"           in a: meta_parts.append(f"🕐 {a['window']}")
        if "rate_pct"         in a: meta_parts.append(f"📈 {a['rate_pct']}% complaint rate")
        if "product"          in a: meta_parts.append(f"📦 {a['product']}")
        if "complaints"       in a: meta_parts.append(f"⚠️ {a['complaints']} complaints")
        if "negative_rate_pct" in a: meta_parts.append(f"😞 {a['negative_rate_pct']}% negative")

        meta_html = ("".join(f"<span>{p}</span>" for p in meta_parts)
                     if meta_parts else "")

        cards.append(f"""
        <div class="alert-card {sev}">
          <div class="alert-card-head">
            <span class="alert-type">{type_str}</span>
            <span class="alert-sev">{sev_label}</span>
          </div>
          <div class="alert-msg">{a.get('message', '')}</div>
          {"<div class='alert-meta'>" + meta_html + "</div>" if meta_html else ""}
        </div>""")

    return f'<div class="alerts-grid">{"".join(cards)}</div>'


def build_urgency_section(hi_urgency: list) -> str:
    """
    Renders the list of messages classified as urgency=high in Stage 2.2.
    These are the items that need a human response as soon as possible.
    """
    if not hi_urgency:
        return """
        <div class="empty-state">
          <span class="icon">✅</span>
          <div>No high-urgency messages today.</div>
        </div>"""

    items = []
    for m in hi_urgency:
        product_span   = (f'<span>📦 {m["product"]}</span>'
                          if m.get("product") else "")
        timestamp_span = (f'<span>🕐 {m["timestamp"]}</span>'
                          if m.get("timestamp") else "")
        channel_span   = (f'<span>📡 {m["channel"]}</span>'
                          if m.get("channel") else "")
        items.append(f"""
        <div class="urgency-item">
          <div class="urgency-badge">URGENT</div>
          <div style="flex:1">
            <div class="urgency-summary">{m.get('summary', 'No summary available')}</div>
            <div class="urgency-meta">
              <span>🆔 {m.get('message_id', '—')}</span>
              {timestamp_span}
              {channel_span}
              <span>{m.get('category', '')}</span>
              {product_span}
            </div>
          </div>
        </div>""")

    return f'<div class="urgency-list">{"".join(items)}</div>'


def build_sentiment_section(sentiment: dict) -> str:
    """
    Renders:
      1. Horizontal bar chart for positive / neutral / negative split
      2. Category breakdown table (Complaint / Inquiry / etc.)
    """
    es   = sentiment.get("executive_summary", {})
    cats = sentiment.get("category_breakdown", {})
    sent = es.get("overall_sentiment", {})

    # Calculate percentage splits
    total   = sum(sent.values()) or 1
    pos_pct = round(sent.get("positive", 0) / total * 100)
    neu_pct = round(sent.get("neutral",  0) / total * 100)
    neg_pct = round(sent.get("negative", 0) / total * 100)

    # ── Sentiment bars ──
    bars = f"""
    <div class="sentiment-container">
      <div style="font-size:13px;font-weight:600;color:var(--text-sub);margin-bottom:18px;">
        Daily Sentiment Distribution — {es.get('total_messages', 0)} messages
      </div>
      <div class="sentiment-row">
        <div class="sentiment-label">Positive </div>
        <div class="bar-wrap"><div class="bar-fill bar-positive" style="width:{pos_pct}%"></div></div>
        <div class="bar-pct" style="color:var(--green)">{pos_pct}%</div>
      </div>
      <div class="sentiment-row">
        <div class="sentiment-label">Neutral </div>
        <div class="bar-wrap"><div class="bar-fill bar-neutral" style="width:{neu_pct}%"></div></div>
        <div class="bar-pct" style="color:var(--salla-teal)">{neu_pct}%</div>
      </div>
      <div class="sentiment-row">
        <div class="sentiment-label">Negative </div>
        <div class="bar-wrap"><div class="bar-fill bar-negative" style="width:{neg_pct}%"></div></div>
        <div class="bar-pct" style="color:var(--red)">{neg_pct}%</div>
      </div>
    </div>"""

    # ── Category breakdown table ──
    # Order categories by operational priority (complaints first)
    cat_order = ["Complaint", "Inquiry", "Transactional Request", "Suggestion"]
    cat_pill  = {
        "Complaint":             "pill-red",
        "Inquiry":               "pill-teal",
        "Transactional Request": "pill-yellow",
        "Suggestion":            "pill-green",
    }

    rows = ""
    for c in cat_order:
        d   = cats.get(c, {})
        cnt = d.get("count", 0)
        s   = d.get("sentiment", {})
        hiu = d.get("urgency", {}).get("high", 0)
        hiu_html = (f'<span class="pill pill-red">{hiu}</span>'
                    if hiu else '<span style="color:var(--text-muted)">—</span>')
        rows += f"""
        <tr>
          <td><span class="pill {cat_pill[c]}">{c}</span></td>
          <td><strong style="font-size:16px">{cnt}</strong></td>
          <td style="color:var(--green);font-weight:600">{s.get('positive',0)}</td>
          <td style="color:var(--salla-teal);font-weight:600">{s.get('neutral',0)}</td>
          <td style="color:var(--red);font-weight:600">{s.get('negative',0)}</td>
          <td>{hiu_html}</td>
        </tr>"""

    table = f"""
    <div class="data-table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th>Category</th><th>Count</th>
            <th> Positive</th><th> Neutral</th><th> Negative</th>
            <th>⚡ High Urgency</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""

    return bars + table


def build_pricing_section(recs: list) -> str:
    """
    Renders one card per product with its pricing recommendation.
    Card background tint signals action type:
      - Green  → INCREASE (price should go up)
      - Red    → BLOCKED  (increase wanted but HC-2 blocked it)
      - Amber  → DATA_INCOMPLETE (missing price or cost)
      - White  → HOLD (no change needed)
    """
    if not recs:
        return '<div class="empty-state"><span class="icon">📊</span><div>No pricing data available.</div></div>'

    # Sort: actionable items first
    priority = {"INCREASE": 0, "BLOCKED": 1, "DECREASE": 2, "HOLD": 3, "DATA_INCOMPLETE": 4}
    sorted_recs = sorted(recs, key=lambda r: priority.get(r.get("action", ""), 9))

    # Icon and pill colour mapping per action type
    action_style = {
        "INCREASE":        ("⬆️", "pill-green"),
        "DECREASE":        ("⬇️", "pill-blue"),
        "HOLD":            ("⏸️", "pill-grey"),
        "BLOCKED":         ("🚫", "pill-red"),
        "DATA_INCOMPLETE": ("❓", "pill-yellow"),
    }

    cards = []
    for r in sorted_recs:
        action       = r.get("action", "HOLD")
        icon, pill   = action_style.get(action, ("•", "pill-grey"))
        title        = r.get("title", "Unknown")
        pid          = r.get("product_id", "")
        cur_p        = r.get("current_price")
        rec_p        = r.get("recommended_price")
        cur_m        = r.get("current_margin_pct")
        explanation  = r.get("explanation", "")

        # Truncate explanation for the card (full text is in the JSON report)
        short_expl = explanation[:210] + ("…" if len(explanation) > 210 else "")

        cur_str = f"{cur_p:.2f} SAR" if cur_p else "—"
        rec_str = f"{rec_p:.2f} SAR" if rec_p else "—"
        mar_str = f"{cur_m}%"        if cur_m else "—"

        # Price display block — three variants depending on action
        if action == "INCREASE" and rec_p and cur_p:
            delta = rec_p - cur_p
            prices_block = f"""
            <div class="pc-prices">
              <div class="pc-price-block">
                <div class="pc-price-lbl">Current</div>
                <div class="pc-price-num" style="color:var(--text-muted)">{cur_str}</div>
              </div>
              <div class="pc-arrow">→</div>
              <div class="pc-price-block">
                <div class="pc-price-lbl">Recommended</div>
                <div class="pc-price-num" style="color:var(--green)">{rec_str}</div>
              </div>
              <div style="text-align:center;margin-left:auto">
                <div class="pc-price-lbl">Delta</div>
                <div style="font-size:15px;font-weight:800;color:var(--green)">+{delta:.2f}</div>
              </div>
            </div>"""
        elif cur_p:
            prices_block = f"""
            <div class="pc-prices">
              <div class="pc-price-block">
                <div class="pc-price-lbl">Current Price</div>
                <div class="pc-price-num">{cur_str}</div>
              </div>
              <div style="text-align:center;margin-left:auto">
                <div class="pc-price-lbl">Margin</div>
                <div style="font-size:18px;font-weight:800">{mar_str}</div>
              </div>
            </div>"""
        else:
            prices_block = f"""
            <div class="pc-prices" style="opacity:0.5">
              <div class="pc-price-block">
                <div class="pc-price-lbl">Price / Cost</div>
                <div class="pc-price-num" style="font-size:14px;color:var(--text-muted)">Data unavailable</div>
              </div>
            </div>"""

        cards.append(f"""
        <div class="price-card {action}">
          <div class="pc-head">
            <div>
              <div class="pc-name">{icon} {title}</div>
              <div class="pc-id">Product ID: {pid}</div>
            </div>
            <span class="pill {pill}">{action.replace('_',' ')}</span>
          </div>
          {prices_block}
          <div class="pc-explanation">{short_expl}</div>
        </div>""")

    return f'<div class="pricing-grid">{"".join(cards)}</div>'


def build_catalog_section(catalog: dict) -> str:
    """
    Renders:
      1. Four mini KPI cards (raw rows / rows with issues / duplicates / unique products)
      2. Grid of top issue types with their counts
    """
    summary      = catalog.get("summary", {})
    total        = summary.get("total_raw_rows", 0)
    issues       = summary.get("rows_with_issues", 0)
    dupes        = summary.get("duplicate_rows_to_remove", 0)
    unique       = total - dupes
    issue_counts = summary.get("issue_type_counts", {})

    # Human-readable label for each issue type key
    issue_labels = {
        "ATTRIBUTE_ABBREV":          "Attribute Abbreviations",
        "UNCERTAIN_DESCRIPTION":     "Uncertain Description",
        "CATEGORY_NORMALISED":       "Category Normalised",
        "MISSING_COST":              "Missing Cost",
        "MISSING_CATEGORY":          "Missing Category",
        "INVALID_PRICE":             "Invalid Price",
        "UNCERTAIN_ATTRIBUTE":       "Uncertain Attribute",
        "NON_ENGLISH_DESCRIPTION":   "Non-English Description",
        "MISSING_ATTRIBUTES":        "Missing Attributes",
        "QUALITY_SIGNAL":            "Quality Signal in Desc.",
        "TITLE_TYPO_FIXED":          "Title Typo Fixed",
        "MISSING_DESCRIPTION":       "Missing Description",
        "VAGUE_DESCRIPTION":         "Vague Description",
        "CUSTOMER_REVIEW_EXTRACTED": "Embedded Customer Review",
    }

    # Mini KPIs row
    mini_kpis = f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;">
      <div class="kpi-card red"    style="padding:16px">
        <div class="kpi-label">Raw Rows</div>
        <div class="kpi-value">{total:,}</div>
        <div class="kpi-sub">in source file</div>
      </div>
      <div class="kpi-card yellow" style="padding:16px">
        <div class="kpi-label">Rows With Issues</div>
        <div class="kpi-value">{issues:,}</div>
        <div class="kpi-sub">flagged for review</div>
      </div>
      <div class="kpi-card red"    style="padding:16px">
        <div class="kpi-label">Duplicate Rows</div>
        <div class="kpi-value">{dupes:,}</div>
        <div class="kpi-sub">removed after dedup</div>
      </div>
      <div class="kpi-card green"  style="padding:16px">
        <div class="kpi-label">Unique Products</div>
        <div class="kpi-value" style="color:var(--green)">{unique}</div>
        <div class="kpi-sub">after deduplication</div>
      </div>
    </div>"""

    # Issue cards grid (top 12 issue types)
    issue_cards = "".join(
        f"""
        <div class="issue-card">
          <div class="issue-name">{issue_labels.get(k, k.replace('_',' ').title())}</div>
          <div class="issue-count">{v:,}</div>
        </div>"""
        for k, v in list(issue_counts.items())[:12]
    )

    return mini_kpis + f'<div class="issue-grid">{issue_cards}</div>'


# =============================================================================
# SECTION 5 — Full HTML assembly
# =============================================================================

def build_full_html(catalog: dict, sentiment: dict, pricing: dict) -> str:
    """
    Stitches all section builders together into a single HTML document.
    The file is fully self-contained (inline CSS + JS).
    Google Fonts is the only external dependency (gracefully degrades offline).
    """
    today   = datetime.date.today().strftime("%B %d, %Y")
    now_str = datetime.datetime.now().strftime("%H:%M · %Y-%m-%d")

    alerts     = sentiment.get("alerts", [])
    # Safety-net fuzzy dedup — mirrors the fingerprint logic in sentiment_analysis_ollama.py.
    # Catches near-duplicates that differ only in punctuation or trailing filler phrases
    # (e.g. "??" vs "????", with/without "pls respond asap").
    import re as _re
    def _fp(text: str) -> str:
        t = text.lower()
        t = _re.sub(r"[^a-z0-9 ]", " ", t)
        t = _re.sub(r"\s+", " ", t).strip()
        for filler in ("pls respond asap", "please respond asap",
                        "pls respond", "please respond", "asap", "urgent"):
            if t.endswith(filler):
                t = t[: -len(filler)].strip()
        return t[:60]

    _seen_fps: set = set()
    hi_urgency = []
    for m in sentiment.get("high_urgency_messages", []):
        fp = _fp(m.get("summary", ""))
        if fp not in _seen_fps:
            _seen_fps.add(fp)
            hi_urgency.append(m)
    recs       = pricing.get("recommendations", [])

    # Compute badge stats.
    # "REQUIRE ACTION" counts only Complaint and Transactional Request messages —
    # Inquiries and Suggestions are high-urgency but do not require immediate
    # merchant action in the same way. This keeps the badge count honest.
    ACTION_CATEGORIES = {"Complaint", "Transactional Request"}
    alert_cnt  = len(alerts)
    hi_urg_cnt = sum(
        1 for m in hi_urgency
        if m.get("category") in ACTION_CATEGORIES
    )
    n_increase = sum(1 for r in recs if r["action"] == "INCREASE")
    n_blocked  = sum(1 for r in recs if r["action"] == "BLOCKED")
    n_gap      = sum(1 for r in recs if r["action"] == "DATA_INCOMPLETE")
    pricing_cnt= n_increase + n_blocked + n_gap

    alert_badge   = "badge-red"  if alert_cnt   > 0 else "badge-green"
    urg_badge     = "badge-red"  if hi_urg_cnt  > 0 else "badge-green"
    pricing_badge = "badge-teal" if n_increase  > 0 else "badge-yellow"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Salla Daily Operations Report — {today}</title>

  <!-- DM Sans: clean, modern, excellent for dashboards -->
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">

  <style>
{FULL_CSS}
  </style>
</head>
<body>

<!-- ═══════════════════════════════════════
     STICKY HEADER
═══════════════════════════════════════ -->
<header class="header">
  <div class="header-brand">
    <div class="logo-box">
      <!-- Simplified Salla bag icon -->
      <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path d="M6 2L3 6v14a2 2 0 002 2h14a2 2 0 002-2V6l-3-4z"/>
        <line x1="3" y1="6" x2="21" y2="6" stroke="rgba(255,255,255,0.5)" stroke-width="1.5" fill="none"/>
        <path d="M16 10a4 4 0 01-8 0" stroke="white" stroke-width="1.5" fill="none"/>
      </svg>
    </div>
    <div class="brand-text">
      <div class="name">Salla Merchant Agent</div>
      <div class="sub">Autonomous Operations Report</div>
    </div>
  </div>
  <div class="header-date">
    <div class="date">{today}</div>
    <div class="time">Generated at {now_str}</div>
  </div>
</header>

<!-- ═══════════════════════════════════════
     SECTION NAVIGATION BAR
═══════════════════════════════════════ -->
<nav class="nav-bar">
  <a class="nav-pill active" href="#alerts">🚨 Alerts ({alert_cnt})</a>
  <a class="nav-pill"        href="#urgent">⚡ Urgent ({hi_urg_cnt})</a>
  <a class="nav-pill"        href="#sentiment">💬 Sentiment</a>
  <a class="nav-pill"        href="#pricing">💰 Pricing</a>
  <a class="nav-pill"        href="#catalog">📦 Catalog</a>
</nav>

<!-- ═══════════════════════════════════════
     MAIN CONTENT
═══════════════════════════════════════ -->
<main class="main">

  <!-- Critical banner — only rendered if high-severity alerts exist -->
  {build_critical_banner(alerts)}

  <!-- Five KPI cards -->
  {build_kpi_row(sentiment, catalog, pricing)}


  <!-- ────────────────────────────────
       SECTION 1: Alerts & Anomalies
  ──────────────────────────────────── -->
  <section class="section" id="alerts">
    <div class="section-header">
      <div class="section-icon">🚨</div>
      <div class="section-title">Alerts &amp; Anomalies</div>
      <span class="section-badge {alert_badge}">{alert_cnt} ALERT{'S' if alert_cnt != 1 else ''}</span>
    </div>
    {build_alerts_section(alerts)}
  </section>


  <!-- ────────────────────────────────
       SECTION 2: High-Urgency Messages
  ──────────────────────────────────── -->
  <section class="section" id="urgent">
    <div class="section-header">
      <div class="section-icon">⚡</div>
      <div class="section-title">High-Urgency Customer Messages</div>
      <span class="section-badge {urg_badge}">{hi_urg_cnt} REQUIRE ACTION</span>
    </div>
    {build_urgency_section(hi_urgency)}
  </section>


  <!-- ────────────────────────────────
       SECTION 3: Customer Sentiment
  ──────────────────────────────────── -->
  <section class="section" id="sentiment">
    <div class="section-header">
      <div class="section-icon">💬</div>
      <div class="section-title">Customer Sentiment Analysis</div>
      <span class="section-badge badge-teal">
        {sentiment.get('executive_summary', {}).get('total_messages', 0)} MESSAGES TODAY
      </span>
    </div>
    {build_sentiment_section(sentiment)}
  </section>


  <!-- ────────────────────────────────
       SECTION 4: Pricing Recommendations
  ──────────────────────────────────── -->
  <section class="section" id="pricing">
    <div class="section-header">
      <div class="section-icon">💰</div>
      <div class="section-title">Pricing Recommendations</div>
      <span class="section-badge {pricing_badge}">
        ↑{n_increase} INCREASE · 🚫{n_blocked} BLOCKED · ❓{n_gap} DATA GAP
      </span>
    </div>
    {build_pricing_section(recs)}
  </section>


  <!-- ────────────────────────────────
       SECTION 5: Catalog Health
  ──────────────────────────────────── -->
  <section class="section" id="catalog">
    <div class="section-header">
      <div class="section-icon">📦</div>
      <div class="section-title">Catalog Health</div>
      <span class="section-badge badge-yellow">NEEDS ATTENTION</span>
    </div>
    {build_catalog_section(catalog)}
  </section>

</main>


<!-- ═══════════════════════════════════════
     FOOTER
═══════════════════════════════════════ -->
<footer class="footer">
  <span>Salla Autonomous Merchant Operations Agent · Daily Report</span>
  <span>
    Sources: catalog_quality_report.json · sentiment_report.json · pricing_recommendations.json
  </span>
</footer>


<!-- ═══════════════════════════════════════
     JS: smooth scroll + active nav tracking
═══════════════════════════════════════ -->
<script>
  const pills    = document.querySelectorAll('.nav-pill');
  const sections = document.querySelectorAll('section[id]');

  // Highlight the nav pill matching the currently visible section
  window.addEventListener('scroll', () => {{
    let current = '';
    sections.forEach(s => {{
      if (window.scrollY >= s.offsetTop - 120) current = s.id;
    }});
    pills.forEach(p => {{
      p.classList.toggle('active', p.getAttribute('href') === '#' + current);
    }});
  }});

  // Smooth-scroll to section when nav pill is clicked
  pills.forEach(p => {{
    p.addEventListener('click', e => {{
      e.preventDefault();
      const target = document.querySelector(p.getAttribute('href'));
      if (target) target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
    }});
  }});
</script>

</body>
</html>"""


# =============================================================================
# SECTION 6 — Main entry point
# =============================================================================

def main():
    """
    Runs the full report pipeline:
      Step 1 — Load the three JSON inputs from the previous pipeline stages
      Step 2 — Build the HTML string
      Step 3 — Write to OUTPUT_PATH
    """
    print("\n" + "=" * 55)
    print("  Salla Merchant Agent — Daily Report Generator")
    print("=" * 55)

    # ── Step 1: load inputs ───────────────────────────────
    print("\n[1/3] Loading pipeline outputs...")
    try:
        catalog   = load_json(CATALOG_PATH)
        sentiment = load_json(SENTIMENT_PATH)
        pricing   = load_json(PRICING_PATH)
        print(f"      ✓ Catalog    : {CATALOG_PATH}")
        print(f"      ✓ Sentiment  : {SENTIMENT_PATH}")
        print(f"      ✓ Pricing    : {PRICING_PATH}")
    except FileNotFoundError as e:
        print(f"\n   Error: {e}")
        return

    # ── Step 2: build HTML ───────────────────────────────
    print("\n[2/3] Building report HTML...")
    html = build_full_html(catalog, sentiment, pricing)

    # ── Step 3: write file ───────────────────────────────
    print(f"\n[3/3] Writing report → {OUTPUT_PATH}")
    Path(OUTPUT_PATH).write_text(html, encoding="utf-8")

    # Print a quick summary to the console
    alerts  = sentiment.get("alerts", [])
    hi_urg  = sentiment.get("high_urgency_messages", [])
    recs    = pricing.get("recommendations", [])
    n_inc   = sum(1 for r in recs if r["action"] == "INCREASE")
    n_blk   = sum(1 for r in recs if r["action"] == "BLOCKED")

    print(f"\n   Report summary:")
    print(f"     Alerts           : {len(alerts)}")
    print(f"     High-urgency msgs: {len(hi_urg)}")
    print(f"     Price increases  : {n_inc}")
    print(f"     Blocked (HC-2)   : {n_blk}")
    print(f"\n   Report ready: {OUTPUT_PATH}")
    print(f"     Open the file in any browser to view it.")
    print("=" * 55 + "\n")


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()