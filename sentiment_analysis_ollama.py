"""
=============================================================================
 Customer Message Analysis — Salla Autonomous Merchant Operations Agent
 LOCAL LLM EDITION  (Ollama  ·  llama3.2:3b recommended)
=============================================================================

 HOW TO RUN:
   1.  Install Ollama      →  https://ollama.com/download
   2.  Pull the model      →  ollama pull llama3.2:3b
   3.  Start the server    →  ollama serve
   4.  Run this script     →  python sentiment_analysis_ollama.py

 CLASSIFIER PRIORITY:
   1. Ollama (local LLM)   — used when `ollama serve` is running
   2. Rule-based fallback  — kicks in automatically if Ollama is unreachable
      (no crash, no interruption — the pipeline always completes)

 KEY OPTIMISATION — DEDUP BEFORE CLASSIFYING:
   In a real customer inbox you often get the same message sent many times
   (templates, copy-paste complaints, bots).  Calling the LLM once per
   UNIQUE message and then broadcasting the result to every duplicate means
   5,000 rows with only 32 unique templates finishes in seconds, not hours.

   Pipeline:
     load CSV  →  deduplicate  →  classify UNIQUE msgs  →
     broadcast results  →  aggregate  →  detect anomalies  →  write reports

 INPUTS:
   CSV with columns:  message_id, channel, message
   (timestamp column is optional — used if present)

 OUTPUTS:
   sentiment_report.json      structured report with alerts
   messages_classified.csv    every row with category/sentiment/urgency
=============================================================================
"""

# ── Standard library only — zero pip installs needed ─────────────────────────
import csv
import json
import re
import time
import datetime
import urllib.request
from collections import defaultdict
from pathlib import Path


# =============================================================================
# SECTION 0 — File paths  (edit these to match your setup)
# =============================================================================

INPUT_CSV       = "customer_messages.csv"         
OUTPUT_JSON     = "sentiment_report.json"           
OUTPUT_CSV      = "messages_classified.csv"         

OLLAMA_MODEL    = "llama3.2:3b"                     
OLLAMA_URL      = "http://localhost:11434/api/generate"
OLLAMA_TIMEOUT  = 30                               


# =============================================================================
# SECTION 1 — Config & constants
# =============================================================================

VALID_CATEGORIES = {"Inquiry", "Complaint", "Suggestion", "Transactional Request"}
VALID_SENTIMENTS = {"positive", "neutral", "negative"}
VALID_URGENCIES  = {"low", "medium", "high"}

# Product names from the catalogue  used to extract product mentions
KNOWN_PRODUCTS = [
    "Wireless EarBud Pro", "EarBud Pro", "EarBuds",
    "Portable Blender", "Blender",
    "Coffee Press",
    "Slim Fit T-shirt", "Slim Fit Tee", "T-shirt", "Tee",
    "Kids Sneakers", "Sneakers",
    "Foldable Table", "Table",
    "3pc Cook Set", "Cook Set",
]

# Anomaly detection thresholds
COMPLAINT_RATE_ALERT   = 0.30   # flag if overall complaint rate exceeds 30%
REPEATED_MSG_THRESHOLD = 10     # flag if the same message appears ≥ N times


# =============================================================================
# SECTION 2 — Load CSV
# =============================================================================

def load_messages(filepath: str) -> list[dict]:
    """
    Read the customer messages CSV.
    Returns a list of dicts  one per row.
    """
    with open(filepath, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# =============================================================================
# SECTION 3 — Deduplication helper
# =============================================================================

def deduplicate_messages(rows: list[dict]) -> tuple[list[str], dict[str, int]]:
    """
    Extract unique message texts and count how many times each appears.

    Returns:
        unique_texts  — list of distinct message strings (order-preserving)
        counts        — {message_text: occurrence_count}

    Why this matters:
        If the inbox has 5,000 rows but only 32 unique message templates,
        we only need to call the LLM 32 times  not 5,000.
        The result is then broadcast to every matching row for free.
    """
    counts: dict[str, int] = defaultdict(int)
    seen:   list[str]      = []

    for row in rows:
        text = row["message"].strip()
        if counts[text] == 0:          # first time we see this exact text
            seen.append(text)
        counts[text] += 1

    return seen, dict(counts)


# =============================================================================
# SECTION 4A — Ollama classifier
# =============================================================================

# System prompt — tight JSON contract so the model always returns parseable output
CLASSIFIER_SYSTEM = """You are a customer service analyst for an e-commerce merchant.

Classify the customer message below and return ONLY a JSON object with exactly these keys:
{
  "category":         one of ["Inquiry", "Complaint", "Suggestion", "Transactional Request"],
  "sentiment":        one of ["positive", "neutral", "negative"],
  "urgency":          one of ["low", "medium", "high"],
  "product_mentioned": the product name if you can identify one, otherwise null
}

Urgency rules:
  high   — safety issue, device dead on arrival, charged twice, urgent keyword
  medium — delivery delay, wrong item, refund/exchange request, defective product
  low    — general inquiry, suggestion, minor order change

Return ONLY the JSON object. No explanation, no markdown fences, nothing else."""


def classify_via_ollama(message: str) -> dict | None:
    """
    Send one message to the local Ollama server and parse the JSON response.

    Returns a classification dict on success, or None on any failure.
    Failure triggers a silent fallback to the rule based classifier 
    the pipeline never crashes because Ollama is unavailable.
    """
    payload = json.dumps({
        "model":  OLLAMA_MODEL,
        "prompt": CLASSIFIER_SYSTEM + "\n\nMessage: " + message,
        "stream": False,
        "format": "json",    # tells Ollama to enforce JSON output
    }).encode("utf-8")

    req = urllib.request.Request(
        OLLAMA_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT) as resp:
            body   = json.loads(resp.read().decode("utf-8"))
            result = json.loads(body["response"])
            result["classifier"] = f"ollama/{OLLAMA_MODEL}"
            return result

    except urllib.error.URLError:
        # Ollama server not running  fall back gracefully
        return None
    except (json.JSONDecodeError, KeyError):
        # Model returned malformed JSON  fall back gracefully
        return None
    except Exception:
        return None


# =============================================================================
# SECTION 4B — Rule-based fallback classifier
# =============================================================================
#
# Used automatically when Ollama is unreachable or returns bad output.
# Keyword scoring approach: each signal pattern adds weight to a category/sentiment.
# The label with the highest total score wins.
# =============================================================================

# (pattern, label, weight) — category signals
CATEGORY_SIGNALS: list[tuple[str, str, int]] = [
    # Complaints
    (r"\b(broke|broken|cracked|defect|faulty|damage)\b",       "Complaint", 3),
    (r"\b(disappoint|terrible|horrible|awful|worst|useless)\b", "Complaint", 3),
    (r"\bnot work(ing)?\b",                                     "Complaint", 3),
    (r"\bstopped work",                                         "Complaint", 3),
    (r"\bwrong (item|size|order)\b",                            "Complaint", 3),
    (r"\b(leak|buzz)\b",                                        "Complaint", 3),
    (r"\bwhere.{0,10}my order\b",                               "Complaint", 3),
    (r"\bstill haven.?t\b",                                     "Complaint", 2),
    (r"\b(poor quality|cheap|peeling|faded|rusty|itchy)\b",     "Complaint", 2),
    (r"\blids? don.?t fit\b",                                   "Complaint", 2),
    # Transactional requests
    (r"\brefund\b",                                             "Transactional Request", 3),
    (r"\breturn\b",                                             "Transactional Request", 3),
    (r"\bcancel",                                               "Transactional Request", 3),
    (r"\b(exchange|replace)\b",                                 "Transactional Request", 3),
    (r"\bchange the size\b",                                    "Transactional Request", 3),
    (r"\bchange.{0,20}order\b",                                 "Transactional Request", 2),
    (r"\border #",                                              "Transactional Request", 2),
    # Suggestions
    (r"\bshould (add|offer|consider)\b",                        "Suggestion", 3),
    (r"\bit would be (great|nice)\b",                           "Suggestion", 3),
    (r"\bplease (add|include|offer|consider)\b",                "Suggestion", 2),
    (r"\bmore (colors?|colours?|sizes?)\b",                     "Suggestion", 2),
    # Inquiries
    (r"\b(compatible|warranty|material|ship)\b",                "Inquiry", 2),
    (r"\b(how long|how much|what sizes?|what colours?)\b",      "Inquiry", 2),
    (r"\bwondering\b",                                          "Inquiry", 2),
    (r"\bmanual.{0,20}(clear|not)\b",                           "Inquiry", 2),
    (r"\bhow do i\b",                                           "Inquiry", 3),
    (r"\bpair.{0,20}(earbuds|earbud)\b",                        "Inquiry", 2),
]

# (pattern, label, weight) — sentiment signals
SENTIMENT_SIGNALS: list[tuple[str, str, int]] = [
    (r"\b(disappoint|terrible|horrible|awful|worst|angry|ridic|itchy)\b", "negative", 3),
    (r"\b(defect|broke|broken|faulty|useless|stuck|buzz|peeling)\b",      "negative", 2),
    (r"\b(wrong|missing|delayed|rusty|faded)\b",                           "negative", 1),
    (r"\b(love|great|excellent|amazing|perfect|fantastic)\b",              "positive", 3),
    (r"\b(nice|happy|enjoy|good|recommend|wish)\b",                        "positive", 2),
]

# (pattern, urgency_level) - first match wins
URGENCY_SIGNALS: list[tuple[str, str]] = [
    (r"\b(URGENT|urgent|buzz.{0,20}charg|safety|charged twice)\b", "high"),
    (r"\bstopped work",                                             "high"),
    (r"\bPls respond asap\b",                                       "high"),
    (r"\b(refund|return|cancel|broken|defect|delay|wrong item)\b",  "medium"),
    (r"\bwhere.{0,10}my order\b",                                   "medium"),
]


def _score_signals(text: str, signals: list[tuple]) -> dict[str, int]:
    """Sum the weights of all matching patterns, grouped by label."""
    scores: dict[str, int] = defaultdict(int)
    for pattern, label, weight in signals:
        if re.search(pattern, text, re.IGNORECASE):
            scores[label] += weight
    return dict(scores)


def _extract_product(text: str) -> str | None:
    """Return the first known product name found in the message text."""
    for product in KNOWN_PRODUCTS:
        if product.lower() in text.lower():
            return product
    return None


def _make_summary(text: str, category: str) -> str:
    """Build a short plain language summary sentence."""
    prefix = {
        "Complaint":             "Customer reports",
        "Inquiry":               "Customer asks about",
        "Suggestion":            "Customer suggests",
        "Transactional Request": "Customer requests",
    }.get(category, "Customer:")
    snippet = re.sub(r"order #\w+", "", text[:80]).strip().rstrip(".!?").lower()
    return f"{prefix} {snippet}"


def classify_rule_based(message: str) -> dict:
    """
    Classify a single message using keyword scoring.
    Always returns a valid result  used when Ollama is unavailable.
    """
    cat_scores  = _score_signals(message, CATEGORY_SIGNALS)
    sent_scores = _score_signals(message, SENTIMENT_SIGNALS)

    category  = max(cat_scores,  key=lambda k: cat_scores[k])  if cat_scores  else "Inquiry"
    sentiment = max(sent_scores, key=lambda k: sent_scores[k]) if sent_scores else "neutral"

    urgency = "low"
    for pattern, level in URGENCY_SIGNALS:
        if re.search(pattern, message, re.IGNORECASE):
            urgency = level
            break

    return {
        "category":          category,
        "sentiment":         sentiment,
        "urgency":           urgency,
        "summary":           _make_summary(message, category),
        "product_mentioned": _extract_product(message),
        "classifier":        "rule_based",
    }


# =============================================================================
# SECTION 5 — Unified classifier dispatcher (Ollama → rule based)
# =============================================================================

def classify_message(message: str) -> dict:
    """
    Try Ollama first; fall back to rule based on any failure.
    Validates and sanitises the result before returning.
    """
    result = classify_via_ollama(message)

    if result is None:
        result = classify_rule_based(message)
    else:
        # Ollama succeeded — fill in the summary (LLM doesn't produce it)
        result["summary"]           = _make_summary(message, result.get("category","Inquiry"))
        result["product_mentioned"] = result.get("product_mentioned") or _extract_product(message)

    # Validate every field against allowed values
    if result.get("category")  not in VALID_CATEGORIES: result["category"]  = "Inquiry"
    if result.get("sentiment") not in VALID_SENTIMENTS: result["sentiment"] = "neutral"
    if result.get("urgency")   not in VALID_URGENCIES:  result["urgency"]   = "low"

    return result


# =============================================================================
# SECTION 6 — THE KEY OPTIMISATION: classify unique messages only
# =============================================================================

def classify_all_deduped(rows: list[dict]) -> list[dict]:
    """
    1. Find every UNIQUE message text in the dataset.
    2. Classify each unique text exactly ONCE.
    3. Copy (broadcast) the result to every row that has the same text.

    Example:
        5,000 rows  ×  32 unique templates  →  32 LLM calls  (not 5,000!)

    This makes local LLMs practical even on slow hardware.
    """
    unique_texts, counts = deduplicate_messages(rows)
    total_unique = len(unique_texts)
    total_rows   = len(rows)

    print(f"  Total rows    : {total_rows:,}")
    print(f"  Unique messages: {total_unique}")
    print(f"  LLM calls needed: {total_unique}  (saved {total_rows - total_unique:,} calls)")
    print()

    # Step 1 — classify each unique text
    cache: dict[str, dict] = {}
    ollama_used   = 0
    fallback_used = 0

    for i, text in enumerate(unique_texts):
        print(f"  [{i+1}/{total_unique}] classifying … ", end="", flush=True)
        result = classify_message(text)
        cache[text] = result

        used = result.get("classifier", "rule_based")
        if "ollama" in used:
            ollama_used += 1
            print(f"✓ ollama  → {result['category']}")
        else:
            fallback_used += 1
            print(f"✓ rules   → {result['category']}")

    # Step 2 — broadcast cached result to every row
    for row in rows:
        text   = row["message"].strip()
        result = cache.get(text, classify_rule_based(text))
        row.update(result)

    print()
    print(f"  Classified via Ollama     : {ollama_used}")
    print(f"  Classified via rule-based : {fallback_used}")
    print(f"  Total rows enriched       : {total_rows:,}")

    return rows


# =============================================================================
# SECTION 7 — Aggregate statistics
# =============================================================================

def build_category_stats(classified: list[dict]) -> dict:
    """
    Count messages by category, with sentiment and urgency breakdowns.

    Deduplicates by message text before counting so that repeated templates
    (e.g. 500 identical "where is my order?" rows) do not inflate counts.
    Each unique message contributes exactly once.
    """
    stats = {
        cat: {"count": 0, "sentiment": defaultdict(int), "urgency": defaultdict(int)}
        for cat in VALID_CATEGORIES
    }
    seen_texts: set[str] = set()
    for msg in classified:
        text = msg.get("message", "").strip()
        if text in seen_texts:
            continue
        seen_texts.add(text)

        cat  = msg.get("category", "Inquiry")
        sent = msg.get("sentiment", "neutral")
        urg  = msg.get("urgency",   "low")
        stats[cat]["count"] += 1
        stats[cat]["sentiment"][sent] += 1
        stats[cat]["urgency"][urg]    += 1

    return {
        cat: {
            "count":     d["count"],
            "sentiment": dict(d["sentiment"]),
            "urgency":   dict(d["urgency"]),
        }
        for cat, d in stats.items()
    }


def build_product_stats(classified: list[dict]) -> dict:
    """
    Count mentions, complaints, and negative sentiment per product.

    Operates on UNIQUE message texts only to avoid duplicate rows distorting
    the neg_rate denominator.

    Problem with raw rows: "blender stopped working" appearing 500 times
    with sentiment=neutral gives complaints 500 but negative-0 → 0% negative,
    which looks like a data error in the alert card.

    Fix: deduplicate by message text first. Each unique message template
    contributes exactly once to product stats  giving accurate sentiment
    ratios that reflect distinct complaint patterns, not copy-paste volume.
    """
    product_stats: dict[str, dict] = defaultdict(
        lambda: {"mentions": 0, "negative": 0, "complaints": 0}
    )

    seen_texts: set[str] = set()
    for msg in classified:
        text = msg.get("message", "").strip()
        if text in seen_texts:
            continue
        seen_texts.add(text)

        product = msg.get("product_mentioned")
        if not product:
            continue
        product_stats[product]["mentions"] += 1
        if msg.get("sentiment") == "negative":
            product_stats[product]["negative"] += 1
        if msg.get("category") == "Complaint":
            product_stats[product]["complaints"] += 1

    return dict(sorted(product_stats.items(), key=lambda x: -x[1]["complaints"]))


# =============================================================================
# SECTION 8 — Anomaly detection
# =============================================================================

def detect_anomalies(classified: list[dict],
                     product_stats: dict,
                     counts: dict[str, int]) -> list[dict]:
    """
    Three detectors run in sequence:
      A. Overall complaint rate spike
      B. Per-product quality / sentiment alerts
      C. Repeated message pattern (bots / templates)
    """
    alerts = []

    # A — Overall complaint rate
    # Deduplicate by message text before counting to avoid duplicate rows
    # inflating both the numerator (complaints) and denominator (total).
    seen_texts_anomaly: set[str] = set()
    unique_classified = []
    for m in classified:
        text = m.get("message", "").strip()
        if text not in seen_texts_anomaly:
            seen_texts_anomaly.add(text)
            unique_classified.append(m)

    total            = len(unique_classified)
    total_complaints = sum(
        1 for m in unique_classified if m.get("category") == "Complaint"
    )
    rate = total_complaints / max(total, 1)

    if rate >= COMPLAINT_RATE_ALERT:
        alerts.append({
            "type":       "COMPLAINT_SPIKE",
            "severity":   "high",
            "rate_pct":   round(rate * 100, 1),
            "complaints": total_complaints,
            "total_msgs": total,
            "message":    (
                f"Complaint rate {rate*100:.1f}% across {total:,} messages "
                f"— well above the {int(COMPLAINT_RATE_ALERT*100)}% alert threshold."
            ),
        })

    # B — Per product alerts
    for product, stats in product_stats.items():
        mentions   = stats["mentions"]
        complaints = stats["complaints"]
        negative   = stats["negative"]
        neg_rate   = negative / max(mentions, 1)

        if complaints >= 3:
            alerts.append({
                "type":              "PRODUCT_QUALITY_ALERT",
                "severity":          "high" if complaints >= 5 else "medium",
                "product":           product,
                "complaints":        complaints,
                "negative_rate_pct": round(neg_rate * 100, 1),
                "message":           (
                    f"'{product}' has {complaints} complaint(s) today "
                    f"({neg_rate*100:.0f}% negative). Review product quality."
                ),
            })
        elif mentions >= 3 and neg_rate >= 0.50:
            alerts.append({
                "type":              "PRODUCT_SENTIMENT_ALERT",
                "severity":          "medium",
                "product":           product,
                "negative_rate_pct": round(neg_rate * 100, 1),
                "message":           (
                    f"'{product}' has a {neg_rate*100:.0f}% negative sentiment rate "
                    f"across {mentions} mention(s)."
                ),
            })

    # C — Repeated messages pattern
    repeated = {msg: cnt for msg, cnt in counts.items()
                if cnt >= REPEATED_MSG_THRESHOLD}
    if repeated:
        alerts.append({
            "type":     "REPEATED_MESSAGES",
            "severity": "medium",
            "count":    len(repeated),
            "message":  (
                f"{len(repeated)} distinct message template(s) each appear "
                f"≥{REPEATED_MSG_THRESHOLD} times — possible automated complaints "
                f"or copy-paste customer campaign."
            ),
            "top_examples": [
                {"text": msg[:80], "count": cnt}
                for msg, cnt in sorted(repeated.items(), key=lambda x: -x[1])[:3]
            ],
        })

    # Sort: high severity first
    alerts.sort(key=lambda a: {"high": 0, "medium": 1, "low": 2}.get(
        a.get("severity", "low"), 2
    ))
    return alerts


# =============================================================================
# SECTION 9 — Build final report
# =============================================================================

def build_report(classified:     list[dict],
                 category_stats: dict,
                 product_stats:  dict,
                 alerts:         list[dict],
                 counts:         dict[str, int]) -> dict:
    """Assemble everything into a single structured report dict."""
    total = len(classified)

    # Deduplicate before computing overall sentiment distribution
    seen_for_sentiment: set[str] = set()
    overall_sentiment: dict[str, int] = defaultdict(int)
    for msg in classified:
        text = msg.get("message", "").strip()
        if text in seen_for_sentiment:
            continue
        seen_for_sentiment.add(text)
        overall_sentiment[msg.get("sentiment", "neutral")] += 1

    dominant = (
        max(overall_sentiment, key=lambda k: overall_sentiment[k])
        if overall_sentiment else "neutral"
    )

    # High urgency messages — fuzzy- eduplicated before capping at 20.
    #
    # Problem: customers send near-identical messages with tiny variations
    # (extra punctuation like "??" vs "????", trailing "pls respond asap", etc.).
    # Exact-string dedup misses these — we need a normalised fingerprint instead.
    #
    # Fingerprint algorithm (no external libraries needed):
    #   1. Lowercase the summary
    #   2. Strip all punctuation and whitespace runs → single spaces
    #   3. Remove filler suffixes people add for urgency ("pls respond asap",
    #      "pls respond", "please respond", "asap", "urgent")
    #   4. Truncate to first 60 chars   catches "same core complaint, different tail"
    #
    # Two messages with the same fingerprint are treated as the same complaint.
    # We keep only the first one seen (lowest message_id = earliest report).

    def _urgency_fingerprint(text: str) -> str:
        """Return a normalised fingerprint for fuzzy deduplication."""
        t = text.lower()
        # Remove punctuation (keep letters, digits, spaces)
        t = re.sub(r"[^a-z0-9 ]", " ", t)
        # Collapse whitespace
        t = re.sub(r"\s+", " ", t).strip()
        # Drop common trailing urgency filler phrases
        for filler in ("pls respond asap", "please respond asap",
                        "pls respond", "please respond", "asap", "urgent"):
            if t.endswith(filler):
                t = t[: -len(filler)].strip()
        # Truncate: first 60 chars capture the core complaint
        return t[:60]

    seen_fingerprints: set[str] = set()
    high_urgency = []
    for m in classified:
        if m.get("urgency") != "high":
            continue
        summary     = m.get("summary", "")
        fingerprint = _urgency_fingerprint(summary)
        if fingerprint in seen_fingerprints:
            continue          # near duplicate skip
        seen_fingerprints.add(fingerprint)
        high_urgency.append({
            "message_id": m.get("message_id", ""),
            "channel":    m.get("channel",    ""),
            "category":   m.get("category"),
            "summary":    summary,
            "product":    m.get("product_mentioned"),
        })
        if len(high_urgency) == 20:
            break             # hard cap at 20 unique urgent messages

    # Classifier breakdown (how many rows were LLM vs rule based)
    classifier_breakdown: dict[str, int] = defaultdict(int)
    for m in classified:
        classifier_breakdown[m.get("classifier", "rule_based")] += 1

    return {
        "report_date":  datetime.date.today().isoformat(),
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "executive_summary": {
            "total_messages":     total,
            "unique_templates":   len(counts),
            "overall_sentiment":  dict(overall_sentiment),
            "dominant_sentiment": dominant,
            "alert_count":        len(alerts),
            "high_urgency_count": len(high_urgency),
        },
        "classifier_breakdown":        dict(classifier_breakdown),
        "category_breakdown":          category_stats,
        "top_products_by_complaints":  product_stats,
        "high_urgency_messages":       high_urgency,
        "alerts":                      alerts,
    }


# =============================================================================
# SECTION 10 — Write outputs
# =============================================================================

def write_json(report: dict, path: str) -> None:
    Path(path).write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

def write_classified_csv(rows: list[dict], path: str) -> None:
    fields = [
        "message_id", "channel", "message",
        "category", "sentiment", "urgency",
        "summary", "product_mentioned", "classifier",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# =============================================================================
# SECTION 11 — Main pipeline
# =============================================================================

def run(input_path:  str = INPUT_CSV,
        json_output: str = OUTPUT_JSON,
        csv_output:  str = OUTPUT_CSV) -> None:
    """
    End-to-end pipeline:
      1. Load messages
      2. Deduplicate  →  classify unique messages  →  broadcast to all rows
      3. Aggregate stats
      4. Detect anomalies
      5. Write JSON report + classified CSV
    """
    print(f"\n{'='*58}")
    print("  SALLA SENTIMENT PIPELINE  —  LOCAL LLM EDITION")
    print(f"  Model : {OLLAMA_MODEL}")
    print(f"{'='*58}\n")

    #  Step 1: load
    print(f"[1/4] Loading messages from '{input_path}' …")
    rows = load_messages(input_path)
    print(f"      → {len(rows):,} rows loaded.\n")

    #  Step 2: dedup + classify 
    print("[2/4] Deduplicating & classifying …")
    _, counts = deduplicate_messages(rows)
    classified = classify_all_deduped(rows)

    #  Step 3: aggregate 
    print("[3/4] Aggregating statistics …")
    category_stats = build_category_stats(classified)
    product_stats  = build_product_stats(classified)
    alerts         = detect_anomalies(classified, product_stats, counts)
    report         = build_report(classified, category_stats, product_stats,
                                  alerts, counts)
    print(f"      → {len(alerts)} alert(s) generated.\n")

    #  Step 4: write outputs 
    print("[4/4] Writing outputs …")
    write_json(report, json_output)
    write_classified_csv(classified, csv_output)
    print(f"      → {json_output}")
    print(f"      → {csv_output}")

    #  Console summary 
    es = report["executive_summary"]
    print(f"\n{'='*58}")
    print("  RESULTS")
    print(f"{'='*58}")
    print(f"  Total messages    : {es['total_messages']:,}")
    print(f"  Unique templates  : {es['unique_templates']}")
    print(f"  Dominant sentiment: {es['dominant_sentiment']}")
    print(f"  High-urgency msgs : {es['high_urgency_count']}")
    print(f"  Alerts raised     : {es['alert_count']}")

    print(f"\n  Category breakdown:")
    for cat, d in category_stats.items():
        bar = "█" * min(int(d["count"] / max(es["total_messages"],1) * 40), 40)
        print(f"    {cat:<26} {d['count']:>5}  {bar}")

    if alerts:
        print(f"\n  Alerts:")
        for a in alerts:
            icon = "" if a.get("severity") == "high" else ""
            print(f"    {icon} [{a.get('severity','?').upper()}] {a['message']}")

    clf = report.get("classifier_breakdown", {})
    print(f"\n  Classifier used:")
    for name, cnt in clf.items():
        print(f"    {name:<30} {cnt:>6} rows")

    print(f"\n{'='*58}\n")

if __name__ == "__main__":
    run(
        input_path  = INPUT_CSV,
        json_output = OUTPUT_JSON,
        csv_output  = OUTPUT_CSV,
    )
