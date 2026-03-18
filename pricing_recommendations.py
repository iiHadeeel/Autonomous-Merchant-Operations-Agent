"""
=============================================================================
 Pricing Recommendations — Salla Autonomous Merchant Operations Agent
 Task 2.3
=============================================================================

 WHAT THIS DOES:
   Combines the cleaned catalog (Task 2.1) with the sentiment report
   (Task 2.2) and an optional pricing_context.csv (competitor prices,
   ratings, market trend) to produce a pricing recommendation for every
   product in the catalog.

 HOW TO RUN:
   python pricing_recommendations.py

 REQUIREMENTS:
   Python 3.10+  ·  zero external libraries  ·  no internet needed

 INPUTS:
   products_cleaned.csv        from Task 2.1
   sentiment_report.json       from Task 2.2
   pricing_context.csv         optional — competitor data
                               columns: product_id, baseline_price, cost,
                                        avg_rating_last_30d, recent_complaints,
                                        competitor_avg_price, trend

 OUTPUTS:
   pricing_recommendations.json   structured recommendations + audit trail
   pricing_recommendations.md     human-readable markdown report

 TWO NON-NEGOTIABLE HARD CONSTRAINTS
 ─────────────────────────────────────────────────────────────────────────────
   HC-1  MINIMUM MARGIN
         A recommended price must NEVER fall below cost ÷ (1 − MIN_MARGIN_PCT).
         If cost data is missing the module refuses any decrease and holds.

   HC-2  SENTIMENT BLOCK
         A price INCREASE is blocked when ANY of the following is true:
           · Product negative-sentiment rate  > NEG_SENTIMENT_BLOCK_PCT
           · Product daily complaint count   >= COMPLAINT_BLOCK_THRESHOLD
           · Market trend field is "negative" or "negative?"
         Detected automatically from the sentiment_report.json produced
         by Task 2.2 — no manual input required.

 DECISION TREE (applied per product, in order)
 ─────────────────────────────────────────────────────────────────────────────
   0  DATA_INCOMPLETE  — missing price AND cost → skip, explain why
   1  HC-1 FLOOR       — current price below cost floor → mandatory increase
   2  HC-2 + thin margin → BLOCKED (want to increase but cannot)
   3  HC-2 + healthy margin → HOLD
   4  Thin margin + stable/rising trend → INCREASE toward target margin
   5  Healthy margin + rising trend + room below competitor ceiling → nudge
   6  Healthy margin, no strong signal → HOLD

 ASSUMPTIONS
 ─────────────────────────────────────────────────────────────────────────────
   · MIN_MARGIN_PCT      = 0.15  (15% gross margin floor — configurable)
   · TARGET_MARGIN_PCT   = 0.35  (35% target gross margin)
   · NEG_BLOCK_PCT       = 0.30  (block increases above 30% negative mentions)
   · COMPLAINT_BLOCK     = 3     (block increases at >= 3 daily complaints)
   · MAX_SINGLE_INCREASE = 0.10  (cap any single-step increase at 10%)
   · Competitor prices stored as "lo–hi" range strings (e.g. "109–140").
     The midpoint is used for positioning; the ceiling caps increases.
   · Product names in the sentiment report are fuzzy-matched to catalog
     titles via PRODUCT_ALIASES to handle short forms ("Blender" →
     "Portable Blender").
   · If pricing_context.csv is absent, the module falls back to catalog
     cost/price only — recommendations still work, just without market data.
=============================================================================
"""

# Standard library only — no pip install needed
import csv
import json
import math
import re
import datetime
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass, field, asdict


# =============================================================================
# SECTION 0 — File paths  (edit these to match your folder)
# =============================================================================

CATALOG_PATH    = "/Users/hadeel/Desktop/salla/products_cleaned.csv"
SENTIMENT_PATH  = "/Users/hadeel/Desktop/salla/sentiment_report.json"
PRICING_CTX     = "/Users/hadeel/Desktop/salla/pricing_context.csv"    # optional — leave as-is if absent
OUTPUT_JSON     = "/Users/hadeel/Desktop/salla/pricing_recommendations.json"
OUTPUT_MD       = "/Users/hadeel/Desktop/salla/pricing_recommendations.md"


# =============================================================================
# SECTION 1 — Configuration  (all thresholds in one place)
# =============================================================================

MIN_MARGIN_PCT          = 0.15   # HC-1: floor margin  (15%)
TARGET_MARGIN_PCT       = 0.35   # ideal gross margin to aim for  (35%)
NEG_SENTIMENT_BLOCK_PCT = 0.30   # HC-2: block increase above this negative rate
COMPLAINT_BLOCK_THRESHOLD = 3    # HC-2: block increase at or above this daily count
MAX_SINGLE_INCREASE     = 0.10   # cap a single price increase at 10%
MAX_SINGLE_DECREASE     = 0.08   # cap a single price decrease at 8%
COMPLAINT_TREND_RATIO   = 2.0    # HC-2 trend proxy: block if today's complaints > ratio × daily avg

# Maps cost values (from pricing_context.csv) to catalog product titles.
# If you add products to the catalog, add an entry here too.
COST_TO_PRODUCT = {
    27.0:  "Slim Fit T-shirt",
    40.0:  "Coffee Press",
    75.0:  "Wireless EarBud Pro",
    95.0:  "Portable Blender",
    110.0: "3pc Cook Set – Steel",
    143.0: "Kids Sneakers",
    280.0: "Foldable Table",
}

# Maps sentiment-report product name variants → canonical catalog title.
# The sentiment classifier often extracts short names ("Blender", "EarBuds").
PRODUCT_ALIASES = {
    "blender":             "Portable Blender",
    "portable blender":    "Portable Blender",
    "coffee press":        "Coffee Press",
    "earbud pro":          "Wireless EarBud Pro",
    "earbuds":             "Wireless EarBud Pro",
    "wireless earbud pro": "Wireless EarBud Pro",
    "t-shirt":             "Slim Fit T-shirt",
    "slim fit t-shirt":    "Slim Fit T-shirt",
    "tee":                 "Slim Fit Tee",
    "slim fit tee":        "Slim Fit Tee",
    "kids sneakers":       "Kids Sneakers",
    "sneakers":            "Kids Sneakers",
    "foldable table":      "Foldable Table",
    "table":               "Foldable Table",
    "cook set":            "3pc Cook Set – Steel",
    "3pc cook set":        "3pc Cook Set – Steel",
}


# =============================================================================
# SECTION 2 — Data structures
# =============================================================================

@dataclass
class ProductSignals:
    """
    All signals gathered for one product before the pricing decision runs.
    Populated from three sources: catalog, sentiment report, pricing context.
    """
    product_id:   str
    title:        str
    category:     str

    # From catalog / pricing context
    current_price:       float
    cost:                float
    current_margin_pct:  float       # (price - cost) / price * 100, or None

    # From sentiment report  (Task 2.2 output)
    total_mentions:    int   = 0
    total_complaints:  int   = 0
    negative_mentions: int   = 0
    neg_rate:          float = 0.0   # negative / total_mentions

    # From pricing context  (optional CSV)
    avg_rating:           object = None   # float or None
    avg_daily_complaints: float  = 0.0
    market_trend:         str    = "unknown"
    competitor_range:     str    = "unknown"
    competitor_lo:        object = None   # float or None
    competitor_hi:        object = None   # float or None

    # Data completeness flags
    price_missing: bool = False
    cost_missing:  bool = False


@dataclass
class PricingRecommendation:
    """
    The final recommendation for one product.
    Every field is written to the JSON output for full transparency.
    """
    product_id:          str
    title:               str
    category:            str

    current_price:       object   # float or None
    cost:                object   # float or None
    current_margin_pct:  object   # float or None

    action:              str      # INCREASE | HOLD | BLOCKED | DATA_INCOMPLETE
    recommended_price:   object   # float or None
    new_margin_pct:      object   # float or None

    avg_rating:          object = None
    market_trend:        str    = "unknown"
    competitor_range:    str    = "unknown"
    avg_daily_complaints:float  = 0.0

    signals_used:        list = field(default_factory=list)
    constraints_checked: list = field(default_factory=list)
    explanation:         str  = ""

    generated_at: str = field(
        default_factory=lambda: datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    )


# =============================================================================
# SECTION 3 — Load inputs
# =============================================================================

def load_catalog(path):
    """
    Load the cleaned product CSV (output of Task 2.1).
    Converts price and cost columns to float where possible.
    """
    products = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            for col in ("price", "cost"):
                raw = row.get(col, "").strip()
                try:
                    row[col] = float(raw) if raw else None
                except ValueError:
                    row[col] = None
            products.append(row)
    return products


def load_sentiment(path):
    """Load the sentiment report JSON produced by Task 2.2."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_pricing_context(path):
    """
    Load the optional pricing_context.csv and aggregate rows by cost value.
    Each cost value represents one product family.

    Returns a dict keyed by cost (float):
      avg_price, avg_rating, avg_complaints, trend,
      competitor_range, competitor_lo, competitor_hi, n_obs

    Returns an empty dict if the file does not exist — the pipeline
    continues with catalog data only.
    """
    if not Path(path).exists():
        return {}

    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    groups = defaultdict(list)
    for r in rows:
        try:
            cost = float(r["cost"])
            groups[cost].append(r)
        except (ValueError, KeyError):
            continue

    result = {}
    for cost, grp in groups.items():
        prices   = [float(r["baseline_price"]) for r in grp]
        ratings  = [float(r["avg_rating_last_30d"]) for r in grp]
        comps    = [int(r["recent_complaints"]) for r in grp]
        trend    = grp[0].get("trend", "unknown")
        comp_str = grp[0].get("competitor_avg_price", "unknown")

        # Parse competitor range string  "109–140" or "109-140"
        comp_lo = comp_hi = None
        m = re.match(r"(\d+)[–\-](\d+)", comp_str)
        if m:
            comp_lo, comp_hi = float(m.group(1)), float(m.group(2))

        result[cost] = {
            "avg_price":       round(sum(prices)  / len(prices),  2),
            "avg_rating":      round(sum(ratings) / len(ratings), 2),
            "avg_complaints":  round(sum(comps)   / len(comps),   1),
            "trend":           trend,
            "competitor_range":comp_str,
            "competitor_lo":   comp_lo,
            "competitor_hi":   comp_hi,
            "n_obs":           len(grp),
        }
    return result


# =============================================================================
# SECTION 4 — Resolve sentiment signals per product
# =============================================================================

def resolve_sentiment_signals(title, prod_sent_map):
    """
    Look up sentiment stats for a catalog product by matching its title
    against all keys in the sentiment report's product stats.

    The sentiment classifier often produces short names ("Blender") rather
    than the full catalog title ("Portable Blender"). PRODUCT_ALIASES maps
    these variants to the canonical title so nothing is missed.

    Returns a dict:  mentions, complaints, negative, neg_rate
    """
    totals = {"mentions": 0, "complaints": 0, "negative": 0}

    for raw_name, stats in prod_sent_map.items():
        canonical = PRODUCT_ALIASES.get(raw_name.lower(), raw_name)
        if canonical.lower() == title.lower() or raw_name.lower() == title.lower():
            for k in totals:
                totals[k] += stats.get(k, 0)

    totals["neg_rate"] = round(
        totals["negative"] / max(totals["mentions"], 1), 3
    )
    return totals


# =============================================================================
# SECTION 5 — Build ProductSignals for every catalog product
# =============================================================================

def build_signals(catalog, sentiment, ctx):
    """
    Merge catalog data, sentiment signals, and (optional) pricing context
    into a ProductSignals object for each product.

    If pricing_context.csv is present, the averaged baseline_price from
    context is preferred over the single catalog value (more robust).
    """
    prod_sent_map = sentiment.get("top_products_by_complaints", {})
    signals_list  = []

    for product in catalog:
        title    = product["title"]
        cat_cost = product.get("cost")

        # Find pricing context for this product using cost as key
        ctx_data = ctx.get(cat_cost) if cat_cost is not None else None

        # Price: prefer context average over single catalog value
        if ctx_data:
            price = ctx_data["avg_price"]
            cost  = cat_cost
        else:
            price = product.get("price")
            cost  = cat_cost

        # Compute current margin
        if price and cost and price > 0:
            margin_pct = round((price - cost) / price * 100, 1)
        else:
            margin_pct = None

        # Resolve sentiment signals via alias matching
        sent = resolve_sentiment_signals(title, prod_sent_map)

        signals_list.append(ProductSignals(
            product_id           = product.get("product_id", ""),
            title                = title,
            category             = product.get("category", ""),
            current_price        = price,
            cost                 = cost,
            current_margin_pct   = margin_pct,
            total_mentions       = sent["mentions"],
            total_complaints     = sent["complaints"],
            negative_mentions    = sent["negative"],
            neg_rate             = sent["neg_rate"],
            avg_rating           = ctx_data["avg_rating"]       if ctx_data else None,
            avg_daily_complaints = ctx_data["avg_complaints"]   if ctx_data else float(sent["complaints"]),
            market_trend         = ctx_data["trend"]            if ctx_data else "unknown",
            competitor_range     = ctx_data["competitor_range"] if ctx_data else "unknown",
            competitor_lo        = ctx_data["competitor_lo"]    if ctx_data else None,
            competitor_hi        = ctx_data["competitor_hi"]    if ctx_data else None,
            price_missing        = price is None,
            cost_missing         = cost  is None,
        ))

    return signals_list


# =============================================================================
# SECTION 6 — Pricing helper utilities
# =============================================================================

def retail_round(price):
    """
    Round to 2 decimal places.
    Whole-number results (e.g. 200.00) become 199.99 — standard retail
    psychological pricing heuristic.
    """
    rounded = round(price, 2)
    if rounded == math.floor(rounded):
        rounded = math.floor(rounded) - 0.01
    return rounded


def gross_margin_pct(price, cost):
    """Return gross margin as a percentage, safe against zero price."""
    if not price or price <= 0:
        return 0.0
    return round((price - cost) / price * 100, 1)


# =============================================================================
# SECTION 7 — Core pricing decision logic
# =============================================================================

def apply_pricing_logic(sig):
    """
    Apply the decision tree to one product's signals.
    Returns a fully-documented PricingRecommendation.

    Every path records:
      signals_used        — what data was considered
      constraints_checked — explicit HC-1 and HC-2 audit entries
      explanation         — plain English rationale for the merchant
    """
    signals_used        = []
    constraints_checked = []

    # ── GATE 0: Data completeness ─────────────────────────────────────────────
    # Without both price and cost, no constraint can be safely checked.
    if sig.price_missing and sig.cost_missing:
        return PricingRecommendation(
            product_id=sig.product_id, title=sig.title, category=sig.category,
            current_price=None, cost=None, current_margin_pct=None,
            action="DATA_INCOMPLETE",
            recommended_price=None, new_margin_pct=None,
            signals_used=["Price missing", "Cost missing"],
            constraints_checked=["HC-1 and HC-2 cannot be evaluated — no price or cost"],
            explanation=(
                f"No recommendation can be made for '{sig.title}' because both the "
                f"selling price and the cost are absent from the catalog. "
                f"Please enter valid price and cost values first."
            ),
        )

    if sig.price_missing:
        min_hint = (
            f"{sig.cost / (1 - MIN_MARGIN_PCT):.2f} SAR"
            if sig.cost else "unknown"
        )
        return PricingRecommendation(
            product_id=sig.product_id, title=sig.title, category=sig.category,
            current_price=None, cost=sig.cost, current_margin_pct=None,
            action="DATA_INCOMPLETE",
            recommended_price=None, new_margin_pct=None,
            signals_used=["Selling price missing from catalog"],
            constraints_checked=["HC-1 unverifiable — no price to compare against cost"],
            explanation=(
                f"'{sig.title}' has no selling price recorded (cost: {sig.cost:.2f} SAR). "
                f"No recommendation is possible. Once a price is set, the minimum price "
                f"satisfying HC-1 ({int(MIN_MARGIN_PCT*100)}% margin) would be {min_hint}."
            ),
        )

    if sig.cost_missing:
        return PricingRecommendation(
            product_id=sig.product_id, title=sig.title, category=sig.category,
            current_price=sig.current_price, cost=None, current_margin_pct=None,
            action="HOLD",
            recommended_price=sig.current_price, new_margin_pct=None,
            signals_used=["Cost missing — HC-1 cannot be verified"],
            constraints_checked=["HC-1 UNVERIFIABLE — holding as safe default"],
            explanation=(
                f"'{sig.title}' is priced at {sig.current_price:.2f} SAR but has no cost "
                f"recorded. HC-1 (never price below cost) cannot be checked, so the price "
                f"is held. Action: enter the cost so margin analysis can proceed."
            ),
        )

    # Both price and cost are available from here on.
    price = sig.current_price
    cost  = sig.cost

    # ── HC-1: compute the minimum allowed price ───────────────────────────────
    min_price = cost / (1 - MIN_MARGIN_PCT)
    constraints_checked.append(
        f"HC-1: minimum floor = {min_price:.2f} SAR  "
        f"(cost {cost:.2f} / {1-MIN_MARGIN_PCT:.2f}, ensures {int(MIN_MARGIN_PCT*100)}% margin)"
    )
    signals_used.append(
        f"Price: {price:.2f} SAR  |  cost: {cost:.2f} SAR  |  "
        f"current margin: {sig.current_margin_pct}%"
    )
    if sig.avg_rating:
        signals_used.append(
            f"Rating: {sig.avg_rating}/5.0  |  avg daily complaints: {sig.avg_daily_complaints}  "
            f"|  trend: {sig.market_trend}  |  competitor: {sig.competitor_range} SAR"
        )
    neg_rate_pct = round(sig.neg_rate * 100, 1)
    signals_used.append(
        f"Sentiment: {sig.total_mentions} mention(s), "
        f"{sig.total_complaints} complaint(s), {neg_rate_pct}% negative"
    )

    # ── HC-2: assess whether a price increase is permitted ────────────────────
    #
    # The spec requires blocking increases when negative sentiment is "trending
    # upward" — not just currently high.  A true trend needs historical data
    # across multiple days, which this prototype does not persist between runs.
    #
    # ASSUMPTION (documented): We approximate the "trending" requirement with
    # two complementary signals that together catch rising complaint patterns:
    #
    #   (a) Snapshot threshold  — today's negative rate > 30% or complaints >= 3
    #       catches a product already in distress.
    #
    #   (b) Complaints-vs-average ratio  — if today's total complaints for this
    #       product exceed 2× the avg_daily_complaints baseline from
    #       pricing_context.csv, we treat this as a rising trend and block the
    #       increase.  Ratio threshold is configurable via COMPLAINT_TREND_RATIO.
    #
    #   (c) Market trend field  — if the pricing_context.csv trend column is
    #       "negative" or "negative?" we block regardless.
    #
    # In a production system, (b) would be replaced by a proper time-series
    # comparison against a rolling N-day window stored in a database.

    trend_blocked = sig.market_trend in ("negative", "negative?")

    # Signal (b): spike ratio — today vs baseline average
    complaint_trending = (
        sig.avg_daily_complaints > 0
        and sig.total_complaints > sig.avg_daily_complaints * COMPLAINT_TREND_RATIO
    )

    hc2_triggered = (
        sig.neg_rate             > NEG_SENTIMENT_BLOCK_PCT
        or sig.total_complaints >= COMPLAINT_BLOCK_THRESHOLD
        or trend_blocked
        or complaint_trending
    )
    hc2_reasons = []
    if sig.neg_rate > NEG_SENTIMENT_BLOCK_PCT:
        hc2_reasons.append(
            f"negative rate {neg_rate_pct}% > {int(NEG_SENTIMENT_BLOCK_PCT*100)}% threshold"
        )
    if sig.total_complaints >= COMPLAINT_BLOCK_THRESHOLD:
        hc2_reasons.append(
            f"{sig.total_complaints} complaints >= threshold of {COMPLAINT_BLOCK_THRESHOLD}"
        )
    if trend_blocked:
        hc2_reasons.append(f"market trend is '{sig.market_trend}'")
    if complaint_trending and f"{sig.total_complaints} complaints" not in " ".join(hc2_reasons):
        hc2_reasons.append(
            f"complaints trending up: {sig.total_complaints} today vs "
            f"{sig.avg_daily_complaints:.1f} daily avg "
            f"({COMPLAINT_TREND_RATIO}× ratio exceeded)"
        )

    constraints_checked.append(
        "HC-2: price increase is "
        + ("BLOCKED — " + "; ".join(hc2_reasons)
           if hc2_triggered
           else (
               f"PERMITTED — neg rate {neg_rate_pct}% <= {int(NEG_SENTIMENT_BLOCK_PCT*100)}%, "
               f"complaints {sig.total_complaints} < {COMPLAINT_BLOCK_THRESHOLD}, "
               f"trend '{sig.market_trend}' is not negative"
           ))
    )

    # ── GATE 1: Current price is BELOW HC-1 floor (mandatory correction) ──────
    if price < min_price:
        new_price  = retail_round(min_price)
        new_margin = gross_margin_pct(new_price, cost)
        return PricingRecommendation(
            product_id=sig.product_id, title=sig.title, category=sig.category,
            current_price=price, cost=cost, current_margin_pct=sig.current_margin_pct,
            action="INCREASE",
            recommended_price=new_price, new_margin_pct=new_margin,
            avg_rating=sig.avg_rating, market_trend=sig.market_trend,
            competitor_range=sig.competitor_range,
            avg_daily_complaints=sig.avg_daily_complaints,
            signals_used=signals_used, constraints_checked=constraints_checked,
            explanation=(
                f"MANDATORY CORRECTION: '{sig.title}' is priced at {price:.2f} SAR — "
                f"below the HC-1 floor of {min_price:.2f} SAR "
                f"(cost {cost:.2f} + {int(MIN_MARGIN_PCT*100)}% margin). "
                f"This is not discretionary — the price MUST be raised to at least "
                f"{new_price:.2f} SAR. New margin: {new_margin}%."
            ),
        )

    # ── BRANCH A: Margin is BELOW target ─────────────────────────────────────
    below_target = (
        sig.current_margin_pct is not None
        and sig.current_margin_pct < TARGET_MARGIN_PCT * 100
    )

    if below_target:
        signals_used.append(
            f"Margin {sig.current_margin_pct}% is below the "
            f"{int(TARGET_MARGIN_PCT*100)}% target — upward pricing pressure"
        )

        if hc2_triggered:
            # Would increase but HC-2 blocks it
            future_hint = retail_round(min(
                cost / (1 - TARGET_MARGIN_PCT),
                price * (1 + MAX_SINGLE_INCREASE)
            ))
            return PricingRecommendation(
                product_id=sig.product_id, title=sig.title, category=sig.category,
                current_price=price, cost=cost, current_margin_pct=sig.current_margin_pct,
                action="BLOCKED",
                recommended_price=price, new_margin_pct=sig.current_margin_pct,
                avg_rating=sig.avg_rating, market_trend=sig.market_trend,
                competitor_range=sig.competitor_range,
                avg_daily_complaints=sig.avg_daily_complaints,
                signals_used=signals_used, constraints_checked=constraints_checked,
                explanation=(
                    f"'{sig.title}' has a thin margin of {sig.current_margin_pct}%, which "
                    f"would normally justify a price increase toward the "
                    f"{int(TARGET_MARGIN_PCT*100)}% target. "
                    f"However, HC-2 blocks any increase because: {'; '.join(hc2_reasons)}. "
                    f"Hold at {price:.2f} SAR. A raise to ~{future_hint:.2f} SAR could be "
                    f"considered once sentiment stabilises (negative rate < "
                    f"{int(NEG_SENTIMENT_BLOCK_PCT*100)}%, complaints < {COMPLAINT_BLOCK_THRESHOLD})."
                ),
            )

        # HC-2 not triggered — safe to increase
        target_price = cost / (1 - TARGET_MARGIN_PCT)
        max_allowed  = price * (1 + MAX_SINGLE_INCREASE)
        new_price    = retail_round(max(min(target_price, max_allowed), min_price))

        # Cap at 5% above competitor ceiling if known
        if sig.competitor_hi and new_price > sig.competitor_hi * 1.05:
            new_price = retail_round(sig.competitor_hi)

        new_margin = gross_margin_pct(new_price, cost)

        comp_note = ""
        if sig.competitor_lo and sig.competitor_hi:
            pos = "within" if sig.competitor_lo <= new_price <= sig.competitor_hi else "slightly above"
            comp_note = f" New price is {pos} the competitor range {sig.competitor_range} SAR."

        return PricingRecommendation(
            product_id=sig.product_id, title=sig.title, category=sig.category,
            current_price=price, cost=cost, current_margin_pct=sig.current_margin_pct,
            action="INCREASE",
            recommended_price=new_price, new_margin_pct=new_margin,
            avg_rating=sig.avg_rating, market_trend=sig.market_trend,
            competitor_range=sig.competitor_range,
            avg_daily_complaints=sig.avg_daily_complaints,
            signals_used=signals_used, constraints_checked=constraints_checked,
            explanation=(
                f"'{sig.title}' has a {sig.current_margin_pct}% margin at {price:.2f} SAR — "
                f"below the {int(TARGET_MARGIN_PCT*100)}% target. "
                f"Sentiment is acceptable ({neg_rate_pct}% negative, "
                f"{sig.total_complaints} complaints, trend '{sig.market_trend}'), "
                f"so HC-2 does not block an increase.{comp_note} "
                f"Recommended: {new_price:.2f} SAR — new margin: {new_margin}%. "
                f"Monitor customer feedback for 48 hours after applying."
            ),
        )

    # ── BRANCH B: Margin AT or ABOVE target ───────────────────────────────────
    signals_used.append(
        f"Margin {sig.current_margin_pct}% meets or exceeds the "
        f"{int(TARGET_MARGIN_PCT*100)}% target — no margin pressure"
    )

    # Opportunistic nudge: rising market + current price well below ceiling
    if (
        not hc2_triggered
        and sig.market_trend == "rising?"
        and sig.competitor_hi
        and price < sig.competitor_hi * 0.92
    ):
        nudge      = retail_round(min(price * 1.04, sig.competitor_hi))
        new_margin = gross_margin_pct(nudge, cost)
        gap_pct    = round((sig.competitor_hi - price) / sig.competitor_hi * 100)
        return PricingRecommendation(
            product_id=sig.product_id, title=sig.title, category=sig.category,
            current_price=price, cost=cost, current_margin_pct=sig.current_margin_pct,
            action="INCREASE",
            recommended_price=nudge, new_margin_pct=new_margin,
            avg_rating=sig.avg_rating, market_trend=sig.market_trend,
            competitor_range=sig.competitor_range,
            avg_daily_complaints=sig.avg_daily_complaints,
            signals_used=signals_used, constraints_checked=constraints_checked,
            explanation=(
                f"'{sig.title}' already meets the margin target ({sig.current_margin_pct}%) "
                f"at {price:.2f} SAR and trend is '{sig.market_trend}'. "
                f"Current price is {gap_pct}% below the competitor ceiling of "
                f"{sig.competitor_hi:.0f} SAR — room for a small opportunistic nudge. "
                f"+4% to {nudge:.2f} SAR stays within the competitor range and lifts "
                f"margin to {new_margin}%."
            ),
        )

    # Default: hold
    comp_note = ""
    if sig.competitor_lo and sig.competitor_hi:
        comp_mid = (sig.competitor_lo + sig.competitor_hi) / 2
        pos = "above" if price > comp_mid else "within"
        comp_note = f" Competitor midpoint: ~{comp_mid:.0f} SAR — current price is {pos} market average."

    hc2_note = (
        f" HC-2 would also block any increase ({'; '.join(hc2_reasons)})."
        if hc2_triggered else ""
    )

    return PricingRecommendation(
        product_id=sig.product_id, title=sig.title, category=sig.category,
        current_price=price, cost=cost, current_margin_pct=sig.current_margin_pct,
        action="HOLD",
        recommended_price=price, new_margin_pct=sig.current_margin_pct,
        avg_rating=sig.avg_rating, market_trend=sig.market_trend,
        competitor_range=sig.competitor_range,
        avg_daily_complaints=sig.avg_daily_complaints,
        signals_used=signals_used, constraints_checked=constraints_checked,
        explanation=(
            f"'{sig.title}' has a comfortable {sig.current_margin_pct}% margin at "
            f"{price:.2f} SAR. Rating {sig.avg_rating or '—'}/5, trend '{sig.market_trend}', "
            f"avg {sig.avg_daily_complaints} daily complaints — no signal strong enough to "
            f"justify a change.{comp_note}{hc2_note} Current price maintained."
        ),
    )


# =============================================================================
# SECTION 8 — Render markdown report
# =============================================================================

def render_markdown(recs):
    """
    Generate a human-readable markdown pricing report including:
      - Constraint reference table
      - Summary of all actions
      - Per-product detail with price/margin table, explanation, audit trail
      - Assumptions & limitations
    """
    today = datetime.date.today().isoformat()
    now   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    action_counts = defaultdict(int)
    for r in recs:
        action_counts[r.action] += 1

    lines = [
        f"# Pricing Recommendations Report — {today}",
        f"_Generated at {now}_",
        "", "---", "",
        "## Hard Constraints (applied to every product)",
        "",
        "| Constraint | Rule |",
        "|-----------|------|",
        f"| **HC-1 Minimum Margin** | Recommended price >= `cost / {1-MIN_MARGIN_PCT:.2f}` — "
        f"guarantees {int(MIN_MARGIN_PCT*100)}% gross margin |",
        f"| **HC-2 Sentiment Block** | Increases blocked when negative sentiment "
        f"> {int(NEG_SENTIMENT_BLOCK_PCT*100)}%, OR complaints >= {COMPLAINT_BLOCK_THRESHOLD}/day, "
        f"OR market trend is negative |",
        "", "---", "",
        "## Summary", "",
        "| Action | Count |",
        "|--------|-------|",
    ]

    icons = {
        "INCREASE":"⬆️","HOLD":"⏸️","BLOCKED":"🚫",
        "DATA_INCOMPLETE":"❓","DECREASE":"⬇️",
    }
    for action, cnt in sorted(action_counts.items()):
        lines.append(f"| {icons.get(action,'•')} {action} | {cnt} |")

    lines += ["", "---", "", "## Product-by-Product Recommendations", ""]

    sort_order = {"INCREASE":0,"BLOCKED":1,"HOLD":2,"DATA_INCOMPLETE":3}
    for rec in sorted(recs, key=lambda r: sort_order.get(r.action, 9)):
        icon    = icons.get(rec.action, "•")
        cur_str = f"{rec.current_price:.2f} SAR"       if rec.current_price       else "—"
        rec_str = f"{rec.recommended_price:.2f} SAR"   if rec.recommended_price   else "—"
        cur_m   = f"{rec.current_margin_pct}%"         if rec.current_margin_pct  else "—"
        new_m   = f"{rec.new_margin_pct}%"             if rec.new_margin_pct       else cur_m
        c_str   = f"{rec.cost:.2f} SAR"                if rec.cost                else "—"
        delta   = (
            f"+{rec.recommended_price - rec.current_price:.2f} SAR"
            if rec.action == "INCREASE" and rec.recommended_price and rec.current_price
            else "—"
        )

        lines += [
            f"### {icon} {rec.title}",
            "",
            "| | Current | Recommended |",
            "|--|--|--|",
            f"| Price | {cur_str} | **{rec_str}** |",
            f"| Margin | {cur_m} | {new_m} |",
            f"| Delta | | {delta} |",
            "",
            "| Detail | |",
            "|--|--|",
            f"| Product ID | `{rec.product_id}` |",
            f"| Category | {rec.category or '—'} |",
            f"| Cost | {c_str} |",
            f"| Avg Rating | {rec.avg_rating or '—'}/5.0 |",
            f"| Market Trend | {rec.market_trend} |",
            f"| Competitor Range | {rec.competitor_range} SAR |",
            f"| Avg Daily Complaints | {rec.avg_daily_complaints} |",
            "",
            "**Explanation**",
            "",
            f"> {rec.explanation}",
            "",
        ]
        if rec.signals_used:
            lines += ["**Signals used:**", ""]
            for s in rec.signals_used:
                lines.append(f"- {s}")
            lines.append("")
        if rec.constraints_checked:
            lines += ["**Constraints audit:**", ""]
            for c in rec.constraints_checked:
                lines.append(f"- {c}")
            lines.append("")
        lines += ["---", ""]

    lines += [
        "## Assumptions & Limitations", "",
        f"- Minimum margin floor: {int(MIN_MARGIN_PCT*100)}% (HC-1). Edit `MIN_MARGIN_PCT` in Section 1.",
        f"- Sentiment block: {int(NEG_SENTIMENT_BLOCK_PCT*100)}% negative rate "
        f"or {COMPLAINT_BLOCK_THRESHOLD}+ daily complaints triggers HC-2.",
        f"- Max single-step increase: {int(MAX_SINGLE_INCREASE*100)}% per cycle.",
        f"- Target margin: {int(TARGET_MARGIN_PCT*100)}%.",
        "- Price decreases require verified competitor data and are not applied in this version.",
        "- Sentiment signals come from `sentiment_report.json` (Task 2.2 output).",
        "",
        "_Auto-generated by Salla Merchant Operations Agent — Task 2.3_",
    ]

    return "\n".join(lines)


# =============================================================================
# SECTION 9 — Main pipeline
# =============================================================================

def run_pricing_pipeline(
    catalog_path     = CATALOG_PATH,
    sentiment_path   = SENTIMENT_PATH,
    pricing_ctx_path = PRICING_CTX,
    json_output      = OUTPUT_JSON,
    md_output        = OUTPUT_MD,
):
    """
    End-to-end pricing pipeline.

      Step 1 — Load catalog, sentiment report, and optional pricing context
      Step 2 — Build ProductSignals for every catalog product
      Step 3 — Apply pricing logic to each product
      Step 4 — Write JSON report and markdown report
    """
    print(f"\n{'='*55}")
    print("  PRICING RECOMMENDATIONS PIPELINE — Task 2.3")
    print(f"{'='*55}\n")

    # Step 1: Load
    print("[1/4] Loading inputs …")
    catalog   = load_catalog(catalog_path)
    sentiment = load_sentiment(sentiment_path)
    ctx       = load_pricing_context(pricing_ctx_path)
    ctx_note  = f"{len(ctx)} product group(s)" if ctx else "not found — using catalog data only"
    print(f"      Catalog     : {len(catalog)} products")
    print(f"      Sentiment   : {sentiment['executive_summary']['total_messages']:,} messages")
    print(f"      Pricing ctx : {ctx_note}\n")

    # Step 2: Build signals
    print("[2/4] Building product signals …")
    signals_list = build_signals(catalog, sentiment, ctx)
    print(f"      → {len(signals_list)} products ready.\n")

    # Step 3: Apply pricing logic
    print("[3/4] Generating recommendations …\n")
    recs = []
    for sig in signals_list:
        rec = apply_pricing_logic(sig)
        recs.append(rec)
        icon = {
            "INCREASE":"⬆️ ","HOLD":"⏸️ ","BLOCKED":"🚫","DATA_INCOMPLETE":"❓",
        }.get(rec.action, "• ")
        info = ""
        if rec.action == "INCREASE" and rec.recommended_price and rec.current_price:
            delta = rec.recommended_price - rec.current_price
            info = (f"  {rec.current_price:.2f} → {rec.recommended_price:.2f} SAR  "
                    f"(+{delta:.2f})  margin {rec.current_margin_pct}% → {rec.new_margin_pct}%")
        elif rec.current_price:
            info = f"  {rec.current_price:.2f} SAR  margin {rec.current_margin_pct or '—'}%"
        print(f"  {icon}{rec.action:<16}  {rec.title:<30}{info}")

    # Step 4: Write outputs
    print(f"\n[4/4] Writing outputs …")
    out = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "constraints": {
            "HC1_min_margin_pct":           int(MIN_MARGIN_PCT * 100),
            "HC2_neg_sentiment_block_pct":  int(NEG_SENTIMENT_BLOCK_PCT * 100),
            "HC2_complaint_block_threshold":COMPLAINT_BLOCK_THRESHOLD,
            "target_margin_pct":            int(TARGET_MARGIN_PCT * 100),
            "max_single_increase_pct":      int(MAX_SINGLE_INCREASE * 100),
        },
        "recommendations": [asdict(r) for r in recs],
    }
    Path(json_output).write_text(json.dumps(out, indent=2, ensure_ascii=False))
    Path(md_output).write_text(render_markdown(recs))
    print(f"      → {json_output}")
    print(f"      → {md_output}")

    # Summary
    n_inc  = sum(1 for r in recs if r.action == "INCREASE")
    n_blk  = sum(1 for r in recs if r.action == "BLOCKED")
    n_hold = sum(1 for r in recs if r.action == "HOLD")
    n_gap  = sum(1 for r in recs if r.action == "DATA_INCOMPLETE")
    print(f"\n{'='*55}")
    print("  RESULTS")
    print(f"{'='*55}")
    print(f"  ⬆️  Increase         : {n_inc}")
    print(f"  🚫 Blocked (HC-2)   : {n_blk}")
    print(f"  ⏸️  Hold              : {n_hold}")
    print(f"  ❓ Data incomplete  : {n_gap}")
    print(f"{'='*55}\n")


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    run_pricing_pipeline()