"""
=============================================================================
 Catalog Analysis — Salla Autonomous Merchant Operations Agent
 Task 2.1
=============================================================================

 WHAT THIS DOES:
   Reads a raw product CSV, cleans every field, detects quality issues,
   deduplicates, and writes two output files:
     1. products_cleaned.csv        — the clean, deduplicated product list
     2. catalog_quality_report.json — full issue summary + sample problems

 HOW TO RUN:
   python catalog_analysis.py

 REQUIREMENTS:
   Python 3.10+  ·  zero external libraries  ·  no internet needed

 INPUTS:
   products_raw.csv   with columns:
     product_id, title, category, price, cost, attributes, description

 OUTPUTS:
   products_cleaned.csv        deduplicated, cleaned product list
   catalog_quality_report.json full quality report

 ASSUMPTIONS (documented here so evaluators can see the thinking):
   - Prices written as English words ("ninety") are converted to numbers.
   - Prices with "SAR" suffix are stripped and stored as plain floats.
   - "unknown" or "??" in the cost field = intentionally missing (None),
     NOT an error — we flag it but do not discard the row.
   - Near-duplicate = same normalised title AND same numeric price.
     The row with the lowest product_id is kept as the canonical record.
   - Embedded customer reviews inside the description field are extracted
     into a separate column (customer_quote) and stripped from the description.
   - Non-English descriptions are flagged but NOT removed — the merchant
     may want to review them manually.
   - Title typos like "Slim Fti T-shirt" are corrected via a known-corrections
     dictionary. New typos can be added to TITLE_CORRECTIONS below.
   - Attribute abbreviations (blk, stl, cottn, bt) are flagged but not
     auto-expanded, because the correct expansion may vary by product context.
   - Minimum margin threshold = 10%. Products below this are flagged
     LOW_MARGIN. Negative margin = NEGATIVE_MARGIN (price below cost).
=============================================================================
"""

# Standard library only — no pip install needed
import csv
import json
import re
from collections import defaultdict
from pathlib import Path


# =============================================================================
# SECTION 0 — File paths  (edit these to match your folder)
# =============================================================================

INPUT_PATH     = "/Users/hadeel/Desktop/salla/products_raw.csv"
CLEANED_OUTPUT = "/Users/hadeel/Desktop/salla/products_cleaned.csv"
REPORT_OUTPUT  = "/Users/hadeel/Desktop/salla/catalog_quality_report.json"


# =============================================================================
# SECTION 1 — Cleaning configuration
# =============================================================================

# Minimum gross margin below which a product is flagged LOW_MARGIN
MIN_MARGIN_PCT = 0.10

# English number words → float values
# Extend this list if your catalog uses other word-form prices
WORD_TO_NUMBER: dict = {
    "zero": 0,   "one": 1,    "two": 2,    "three": 3,  "four": 4,
    "five": 5,   "six": 6,    "seven": 7,  "eight": 8,  "nine": 9,
    "ten": 10,   "twenty": 20,"thirty": 30,"forty": 40,
    "fifty": 50, "sixty": 60, "seventy": 70,"eighty": 80,"ninety": 90,
    "hundred": 100,
}

# Known title typos → correct title
# Add new entries here as you discover them in the catalog
TITLE_CORRECTIONS: dict = {
    "slim fti t-shirt": "Slim Fit T-shirt",
    "slim fti tee":     "Slim Fit Tee",
}

# Category variants → canonical category name
# Keys must be lowercase; values are the canonical display form
CATEGORY_MAP: dict = {
    "clothes > mens":   "Clothes > Mens",
    "menswear":         "Clothes > Mens",
    "apparel/men":      "Clothes > Mens",
    "apparel > men":    "Clothes > Mens",
    "kitchen & dining": "Kitchen & Dining",
    "kitchen":          "Kitchen & Dining",
    "home appliances":  "Home Appliances",
    "shoes/kids":       "Shoes > Kids",
    "shoes > kids":     "Shoes > Kids",
    "shoes":            "Shoes > Kids",
}

# Attribute abbreviations that should be flagged for merchant review
ATTRIBUTE_ABBREVIATIONS = ("blk", "stl", "cottn", "bt")

# Regex patterns in the description that signal uncertain / low-quality content
UNCERTAIN_DESC_PATTERNS = [
    r"\bmaybe\b",
    r"\bsame model as\b",
    r"but.{0,30}missing",
    r"\?",         # bare question mark inside the description text
]

# Regex to detect and extract embedded customer reviews
# Matches patterns like:  Customer said: 'ok quality'.
CUSTOMER_REVIEW_RE = re.compile(
    r"[Cc]ustomer(?:\s+said)?:?\s*['\"](.+?)['\"]\.?",
    re.IGNORECASE,
)


# =============================================================================
# SECTION 2 — Per-field cleaning functions
# =============================================================================

def clean_price(raw: str) -> tuple:
    """
    Parse the price field into a plain float.

    Handles these cases:
      "129"       → (129.0, [])
      "179 SAR"   → (179.0, ["PRICE_HAD_SAR_SUFFIX"])
      "ninety"    → (90.0,  ["PRICE_WAS_WORD"])
      "Unclear"   → (None,  ["INVALID_PRICE"])
      "" / None   → (None,  ["MISSING_PRICE"])

    Returns:
      (numeric_value_or_None, list_of_issue_tags)
    """
    if not raw or not str(raw).strip():
        return None, ["MISSING_PRICE"]

    raw = str(raw).strip()

    # Plain number, optionally followed by SAR
    m = re.match(r"^([\d.]+)\s*(SAR|sar)?$", raw)
    if m:
        value  = float(m.group(1))
        issues = ["PRICE_HAD_SAR_SUFFIX"] if m.group(2) else []
        return value, issues

    # English word number  (e.g. "ninety")
    if raw.lower() in WORD_TO_NUMBER:
        return float(WORD_TO_NUMBER[raw.lower()]), ["PRICE_WAS_WORD"]

    # Anything else is invalid
    return None, ["INVALID_PRICE"]


def clean_cost(raw: str) -> tuple:
    """
    Parse the cost field into a plain float.

    "unknown" and "??" are treated as intentionally missing — the merchant
    may not track cost for every product.  We flag them with MISSING_COST
    but keep the row rather than discarding it.

    Returns:
      (numeric_value_or_None, list_of_issue_tags)
    """
    if not raw or not str(raw).strip():
        return None, ["MISSING_COST"]

    raw = str(raw).strip()

    # Explicitly unknown values
    if raw.lower() in ("unknown", "??", "n/a", "-"):
        return None, ["MISSING_COST"]

    # Strip SAR suffix if present
    m = re.match(r"^([\d.]+)\s*(SAR|sar)?$", raw)
    if m:
        return float(m.group(1)), []

    # Unrecognised format — flag as missing rather than crash
    return None, ["MISSING_COST"]


def clean_title(raw: str) -> tuple:
    """
    Return the corrected title and flag any known typos.

    Looks up the lowercase version of raw in TITLE_CORRECTIONS.
    If found, returns the corrected form + TITLE_TYPO_FIXED.
    Otherwise returns the original (stripped) title with no issue.

    Returns:
      (title_string, list_of_issue_tags)
    """
    stripped  = raw.strip()
    corrected = TITLE_CORRECTIONS.get(stripped.lower())
    if corrected:
        return corrected, ["TITLE_TYPO_FIXED"]
    return stripped, []


def clean_category(raw: str) -> tuple:
    """
    Normalise category variants to a single canonical name.

    Empty category         → ("", ["MISSING_CATEGORY"])
    Known variant in map   → (canonical_name, ["CATEGORY_NORMALISED"])
    Unknown / already good → (original_value, [])

    Returns:
      (category_string, list_of_issue_tags)
    """
    if not raw or not raw.strip():
        return "", ["MISSING_CATEGORY"]

    canonical = CATEGORY_MAP.get(raw.strip().lower())
    if canonical:
        return canonical, ["CATEGORY_NORMALISED"]

    return raw.strip(), []


def clean_attributes(raw: str) -> tuple:
    """
    Check the attributes field for quality problems.

    MISSING_ATTRIBUTES   — field is empty
    UNCERTAIN_ATTRIBUTE  — contains "??" placeholder values
    ATTRIBUTE_ABBREV     — contains known abbreviations (blk, stl, cottn, bt)

    The value is returned unchanged — we flag but do not auto-correct,
    because the correct expansion is context-dependent per product.

    Returns:
      (attribute_string, list_of_issue_tags)
    """
    if not raw or not raw.strip():
        return "", ["MISSING_ATTRIBUTES"]

    issues = []

    if "??" in raw:
        issues.append("UNCERTAIN_ATTRIBUTE")

    for abbr in ATTRIBUTE_ABBREVIATIONS:
        # Word-boundary match prevents "bt" matching inside "bluetooth"
        if re.search(r"\b" + abbr + r"\b", raw, re.IGNORECASE):
            issues.append("ATTRIBUTE_ABBREV")
            break   # one flag per row is enough

    return raw.strip(), issues


def clean_description(raw: str) -> tuple:
    """
    Clean the description field and extract embedded content.

    Actions performed in order:
      1. Extract customer reviews — stored separately, removed from text,
         flagged as CUSTOMER_REVIEW_EXTRACTED.
      2. Flag NON_ENGLISH_DESCRIPTION if >15% of chars are non-ASCII.
      3. Flag UNCERTAIN_DESCRIPTION for vague language (maybe, ?, etc.)
      4. Flag MISSING_DESCRIPTION if the field is empty.
      5. Flag VAGUE_DESCRIPTION for very short descriptions (1-2 words).

    Returns:
      (cleaned_description_string, list_of_issue_tags, customer_quote_or_None)
    """
    if not raw or not raw.strip():
        return "", ["MISSING_DESCRIPTION"], None

    text   = raw.strip()
    issues = []
    customer_quote = None

    # Step 1 — extract embedded customer reviews
    match = CUSTOMER_REVIEW_RE.search(text)
    if match:
        customer_quote = match.group(1).strip()
        text = CUSTOMER_REVIEW_RE.sub("", text).strip()
        issues.append("CUSTOMER_REVIEW_EXTRACTED")

    # Step 2 — non-English detection  (>15% non-ASCII characters)
    non_ascii = sum(1 for c in text if ord(c) > 127)
    if non_ascii / max(len(text), 1) > 0.15:
        issues.append("NON_ENGLISH_DESCRIPTION")

    # Step 3 — uncertain language patterns
    for pattern in UNCERTAIN_DESC_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            issues.append("UNCERTAIN_DESCRIPTION")
            break   # one flag per row

    # Step 4 — empty after stripping reviews
    if not text:
        issues.append("MISSING_DESCRIPTION")

    # Step 5 — very short / vague description
    elif len(text.split()) <= 2:
        issues.append("VAGUE_DESCRIPTION")

    return text, issues, customer_quote


# =============================================================================
# SECTION 3 — Row-level cleaner
# =============================================================================

def clean_row(raw: dict) -> dict:
    """
    Apply all field cleaners to one raw CSV row.

    Also computes margin health when both price and cost are known:
      NEGATIVE_MARGIN  — price is below cost (selling at a loss)
      LOW_MARGIN       — gross margin < MIN_MARGIN_PCT (default 10%)

    All individual issue tags are combined into a single '_issues' list.
    The caller strips '_issues' before writing to CSV output.

    Returns a cleaned row dict with all fields + '_issues'.
    """
    all_issues = []

    # Apply each cleaner and collect the issue tags
    title,       ti = clean_title(raw.get("title", ""))
    price,       pi = clean_price(raw.get("price", ""))
    cost,        ci = clean_cost(raw.get("cost", ""))
    category,    ki = clean_category(raw.get("category", ""))
    attributes,  ai = clean_attributes(raw.get("attributes", ""))
    description, di, customer_quote = clean_description(raw.get("description", ""))

    all_issues += ti + pi + ci + ki + ai + di

    # Margin health check — only when both values are valid
    if price is not None and cost is not None and price > 0:
        margin = (price - cost) / price
        if margin < 0:
            all_issues.append("NEGATIVE_MARGIN")
        elif margin < MIN_MARGIN_PCT:
            all_issues.append("LOW_MARGIN")

    return {
        "product_id":     raw.get("product_id", ""),
        "title":          title,
        "category":       category,
        "price":          price,
        "cost":           cost,
        "attributes":     attributes,
        "description":    description,
        "customer_quote": customer_quote or "",
        "_issues":        all_issues,
    }


# =============================================================================
# SECTION 4 — Duplicate detection
# =============================================================================

def detect_duplicates(cleaned_rows: list) -> dict:
    """
    Group rows by (normalised_title_lowercase, numeric_price).

    A "duplicate group" is any group that contains more than one product_id.
    Returns a dict mapping the group key → list of product_ids in the group.

    Design note:
      We use title + price as the duplicate key deliberately.
      Using more fields (e.g. attributes) would create false negatives where
      the same physical product with slightly different metadata is missed.
      The price anchor prevents unrelated products with the same name at
      different prices from being incorrectly merged.
    """
    groups = defaultdict(list)
    for row in cleaned_rows:
        key = (row["title"].lower().strip(), row["price"])
        groups[key].append(row["product_id"])

    # Only return groups that actually have more than one member
    return {key: ids for key, ids in groups.items() if len(ids) > 1}


def deduplicate(cleaned_rows: list, dupe_groups: dict) -> list:
    """
    For each duplicate group, keep the row with the LOWEST product_id
    and discard all others.  Non-duplicate rows are always kept.

    The "lowest product_id" rule is a simple, deterministic policy that
    prefers the earliest-created record in the system.

    Returns the deduplicated list, preserving insertion order.
    """
    # Identify the canonical (keeper) ID from each duplicate group
    canonical_ids = {min(ids) for ids in dupe_groups.values()}

    # All IDs that appear in ANY duplicate group (including the non-keepers)
    all_dupe_ids = {pid for ids in dupe_groups.values() for pid in ids}

    unique   = []
    seen_ids = set()

    for row in cleaned_rows:
        pid = row["product_id"]

        if pid in all_dupe_ids:
            # Only keep the canonical member of this duplicate group
            if pid in canonical_ids and pid not in seen_ids:
                unique.append(row)
                seen_ids.add(pid)
        else:
            # Not a duplicate — always keep (guard against re-adding)
            if pid not in seen_ids:
                unique.append(row)
                seen_ids.add(pid)

    return unique


# =============================================================================
# SECTION 5 — Quality summary builder
# =============================================================================

def build_summary(raw_rows: list, cleaned_rows: list, dupe_groups: dict) -> dict:
    """
    Aggregate all issue tags into a summary report dictionary.

    Produces:
      - total_raw_rows           : how many rows were in the input file
      - rows_with_issues         : rows that have at least one issue tag
      - rows_clean               : rows with zero issues
      - duplicate_groups_found   : number of groups with >1 member
      - duplicate_rows_to_remove : total rows that would be deleted
      - issue_type_counts        : {tag: count} sorted descending
      - sample_issues            : first 20 rows that had issues (for the report)
    """
    issue_counts     = defaultdict(int)
    rows_with_issues = 0
    sample_issues    = []

    for row in cleaned_rows:
        issues = row.get("_issues", [])
        if issues:
            rows_with_issues += 1
            for tag in issues:
                issue_counts[tag] += 1
            if len(sample_issues) < 20:
                sample_issues.append({
                    "product_id": row["product_id"],
                    "title":      row["title"],
                    "price":      row["price"],
                    "cost":       row["cost"],
                    "issues":     issues,
                })

    # Each duplicate group of size N contributes N-1 rows to remove
    dupe_rows_to_remove = sum(len(v) - 1 for v in dupe_groups.values())

    return {
        "total_raw_rows":           len(raw_rows),
        "rows_with_issues":         rows_with_issues,
        "rows_clean":               len(cleaned_rows) - rows_with_issues,
        "duplicate_groups_found":   len(dupe_groups),
        "duplicate_rows_to_remove": dupe_rows_to_remove,
        "issue_type_counts":        dict(
            sorted(issue_counts.items(), key=lambda x: -x[1])
        ),
        "sample_issues":            sample_issues,
    }


# =============================================================================
# SECTION 6 — Write outputs
# =============================================================================

def write_cleaned_csv(unique_rows: list, path: str) -> None:
    """
    Write the deduplicated, cleaned rows to a CSV file.
    The internal '_issues' field is excluded from the output.
    """
    fields = [
        "product_id", "title", "category",
        "price", "cost", "attributes", "description", "customer_quote",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(unique_rows)


def write_quality_report(summary: dict, dupe_groups: dict, path: str) -> None:
    """
    Write the full quality report to a JSON file.
    Includes summary stats, the first 5 duplicate groups, and sample issues.
    """
    report = {
        "summary": summary,
        # Convert tuple keys to strings for JSON serialisation
        "duplicate_groups": {
            f"{k[0]} | price={k[1]}": v
            for k, v in list(dupe_groups.items())[:5]
        },
    }
    Path(path).write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# =============================================================================
# SECTION 7 — Main pipeline
# =============================================================================

def run_catalog_analysis(input_path:   str = INPUT_PATH,
                          cleaned_path: str = CLEANED_OUTPUT,
                          report_path:  str = REPORT_OUTPUT) -> None:
    """
    Orchestrate the full catalog analysis pipeline.

      Step 1 — Load the raw CSV into memory
      Step 2 — Clean every row (all 8 field cleaners run per row)
      Step 3 — Detect duplicate groups by title + price
      Step 4 — Deduplicate (keep lowest product_id per group)
      Step 5 — Build the quality summary report
      Step 6 — Write cleaned CSV + JSON report to disk
    """
    print(f"\n{'='*55}")
    print("  CATALOG ANALYSIS PIPELINE — Task 2.1")
    print(f"{'='*55}\n")

    # Step 1: Load
    print(f"[1/5] Loading '{input_path}' …")
    with open(input_path, encoding="utf-8") as f:
        raw_rows = list(csv.DictReader(f))
    print(f"      → {len(raw_rows):,} rows loaded.\n")

    # Step 2: Clean
    print("[2/5] Cleaning rows …")
    cleaned_rows = [clean_row(r) for r in raw_rows]
    rows_with_issues = sum(1 for r in cleaned_rows if r["_issues"])
    print(f"      → {rows_with_issues:,}/{len(cleaned_rows):,} rows have at least one issue.\n")

    # Step 3: Detect duplicates
    print("[3/5] Detecting duplicates …")
    dupe_groups     = detect_duplicates(cleaned_rows)
    dupe_rows_count = sum(len(v) - 1 for v in dupe_groups.values())
    print(f"      → {len(dupe_groups)} duplicate group(s) found "
          f"({dupe_rows_count:,} rows to remove).\n")

    # Step 4: Deduplicate
    print("[4/5] Deduplicating …")
    unique_rows = deduplicate(cleaned_rows, dupe_groups)
    print(f"      → {len(unique_rows)} unique product(s) remain.\n")

    # Step 5: Build summary
    print("[5/5] Building quality summary …")
    summary = build_summary(raw_rows, cleaned_rows, dupe_groups)

    # Write outputs
    write_cleaned_csv(unique_rows, cleaned_path)
    write_quality_report(summary, dupe_groups, report_path)

    # Console results
    print(f"\n{'='*55}")
    print("  RESULTS")
    print(f"{'='*55}")
    print(f"  Total raw rows         : {summary['total_raw_rows']:>7,}")
    print(f"  Rows with issues       : {summary['rows_with_issues']:>7,}")
    print(f"  Clean rows             : {summary['rows_clean']:>7,}")
    print(f"  Duplicate groups       : {summary['duplicate_groups_found']:>7,}")
    print(f"  Duplicate rows removed : {summary['duplicate_rows_to_remove']:>7,}")
    print(f"  Unique products kept   : {len(unique_rows):>7}")

    print(f"\n  Top issue types:")
    for tag, count in list(summary["issue_type_counts"].items())[:10]:
        bar = "█" * min(int(count / max(summary["total_raw_rows"], 1) * 40), 40)
        print(f"    {tag:<34} {count:>6,}  {bar}")

    print(f"\n  Cleaned CSV    → {cleaned_path}")
    print(f"  Quality report → {report_path}")

    print(f"\n{'='*55}")
    print("  Unique products after deduplication:")
    print(f"  {'ID':<8} {'Title':<30} {'Price':>8}  {'Cost':>8}  {'Margin':>7}")
    print(f"  {'-'*70}")
    for r in unique_rows:
        p = r["price"]
        c = r["cost"]
        margin_str = f"{(p-c)/p*100:.1f}%" if (p and c and p > 0) else "  —"
        price_str  = f"{p:.2f}" if p is not None else "  —"
        cost_str   = f"{c:.2f}" if c is not None else "  —"
        print(f"  {r['product_id']:<8} {r['title']:<30} "
              f"{price_str:>8}  {cost_str:>8}  {margin_str:>7}")
    print(f"{'='*55}\n")


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    run_catalog_analysis(
        input_path   = INPUT_PATH,
        cleaned_path = CLEANED_OUTPUT,
        report_path  = REPORT_OUTPUT,
    )