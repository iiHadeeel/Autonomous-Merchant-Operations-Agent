# Salla Autonomous Merchant Operations Agent
**Junior Developer Case Study — Technical Submission**

---

## Overview

A fully local pipeline that turns a merchant's raw daily data into a structured, actionable operations report. No cloud APIs required — the LLM runs on-device via Ollama, and everything else uses Python's standard library only.

```
products_raw.csv          ─── Stage 2.1 ──► products_cleaned.csv
customer_messages.csv     ─── Stage 2.2 ──► sentiment_report.json
                          ─── Stage 2.3 ──► pricing_recommendations.json
                          ─── Stage 2.4 ──► daily_report_salla_en.html
```

---

## How to Run

### Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| Ollama | latest (https://ollama.com/download) |
| Model | `llama3.2:3b` |
| External pip installs | **none** |

```bash
# 1. Pull the model
ollama pull llama3.2:3b

# 2. Start the server (keep this running in a separate terminal)
ollama serve
```

### Run the pipeline in order

```bash
# Stage 2.1 — Catalog Analysis
python catalog_analysis.py

# Stage 2.2 — Customer Sentiment Analysis
python sentiment_analysis_ollama.py

# Stage 2.3 — Pricing Recommendations
python pricing_recommendations.py

# Stage 2.4 — Daily Report
python daily_report_ollama.py

# Open the report
open daily_report_salla_en.html        # macOS
start daily_report_salla_en.html       # Windows
```

> **No Ollama?** The sentiment pipeline falls back automatically to a rule-based classifier. Output quality is slightly lower but the pipeline always completes.

---

## File Map

| File | Stage | Description |
|---|---|---|
| `catalog_analysis.py` | 2.1 | Parses, cleans, deduplicates the product CSV |
| `sentiment_analysis_ollama.py` | 2.2 | Classifies customer messages, detects anomalies |
| `pricing_recommendations.py` | 2.3 | Generates pricing decisions with HC-1 / HC-2 enforcement |
| `daily_report_ollama.py` | 2.4 | Renders everything into a single HTML report |
| `observability.py` | all stages | Structured JSONL logger · stdout trace · CLI replay tool |
| `products_raw.csv` | input | Raw product catalog |
| `customer_messages.csv` | input | Raw customer message log |
| `pricing_context.csv` | input (optional) | Competitor prices, ratings, market trend per product |

**Generated outputs** (created when you run the pipeline):

| File | Created by |
|---|---|
| `products_cleaned.csv` | Stage 2.1 |
| `catalog_quality_report.json` | Stage 2.1 |
| `messages_classified.csv` | Stage 2.2 |
| `sentiment_report.json` | Stage 2.2 |
| `pricing_recommendations.json` | Stage 2.3 |
| `pricing_recommendations.md` | Stage 2.3 |
| `daily_report_salla_en.html` | Stage 2.4 |

---


---

## Deliverables checklist

| # | Deliverable | Status | Where |
|---|---|---|---|
| 1 | Working code | Done | All four `.py` pipeline scripts |
| 2 | Architecture diagram | Done | `architecture_diagram.svg` (also in this README below) |
| 3 | LLM integration | Done | Ollama `llama3.2:3b` · `sentiment_analysis_ollama.py` Section 4A |
| 4 | Observability | Done | `observability.py` · outputs `agent_trace.log` (JSONL) |
| 5 | Build notes | Done | This README |

### LLM integration detail (Deliverable 3)

- Provider: **Ollama** (local, no cloud credentials required)
- Model: **llama3.2:3b**
- Used in:  — every unique customer message is classified via a structured JSON prompt asking the model to return , , , and .
- Fallback: if Ollama is unreachable, a rule-based keyword scorer kicks in automatically — the pipeline never crashes.
- Efficiency: deduplication runs before LLM calls, so 5,000 messages with 32 unique templates = only **32 LLM calls** instead of 5,000.

### Observability detail (Deliverable 4)

 instruments every pipeline stage with zero external dependencies:

- **JSONL trace log** () — one structured event per line, easy to grep and parse
- **Colour-coded stdout** — human-readable stage progress with timing
- **Typed helpers**: , , , , , 
- **CLI replay**:   [36mℹ [agent] run_start[0m

  Trace log: agent_trace.log  (1 events)

  TIME                   LEVEL  STAGE                        EVENT                  DETAILS
  ────────────────────────────────────────────────────────────────────────────────────────────────────
  [36m19:26:24               INFO   init                         run_start              [0m pretty-prints the full run history
- Integration snippets for all four scripts are in Section 3 of 
- LangSmith alternative is documented (commented out) in Section 4 of 

## Design Decisions & Assumptions

### Stage 2.1 — Catalog Analysis

| Assumption | Rationale |
|---|---|
| Prices written as English words ("ninety") are converted to numbers | Real merchant CSVs often contain manual entry artifacts |
| `"unknown"` or `"??"` in the cost field = intentionally missing, not an error | Merchants sometimes track margin informally; we flag it but keep the row |
| Near-duplicate = same normalised title AND same numeric price | Two products with the same name but different prices are SKU variants, not duplicates |
| The row with the lowest `product_id` is kept as the canonical record | Lowest ID = earliest entry = most likely the original listing |
| Attribute abbreviations (blk, stl, cottn, bt) are flagged but not auto-expanded | The correct expansion can vary by product context; merchant review is safer |
| Non-English descriptions are flagged but NOT removed | The merchant may be deliberately targeting non-English markets |
| Minimum margin threshold = 10% | Products below this are flagged `LOW_MARGIN`. A common retail floor; configurable via `MIN_MARGIN_PCT` |

### Stage 2.2 — Customer Sentiment Analysis

| Assumption | Rationale |
|---|---|
| Dedup before classifying | With 5,000 messages and only 32 unique templates, classifying unique texts only cuts LLM calls by ~99% with identical output |
| High-urgency messages are fuzzy-deduplicated in the report | Near-identical messages differing only in punctuation ("??" vs "????") or trailing filler ("pls respond asap") should appear as one item, not N items |
| The `summary` field uses a rule-based template (`"Customer reports …"`) rather than asking the LLM to summarise | Consistent format for the report; LLM summaries for short messages added more noise than signal |
| Anomaly thresholds: complaint rate > 30%, repeated message template ≥ 10 occurrences | Common e-commerce operations benchmarks; both are configurable constants |

### Stage 2.3 — Pricing Recommendations

The two hard constraints are enforced before any other logic runs:

**HC-1 (minimum margin):** The recommended price is never allowed to fall below `cost / (1 − 0.15)` — a 15% gross margin floor. If cost data is missing, any decrease is refused and the product is held.

**HC-2 (sentiment block):** A price increase is blocked when ANY of the following is true:
- Product negative-sentiment rate > 30%
- Product daily complaint count ≥ 3
- Market trend field is `"negative"` or `"negative?"`
- Today's complaint count exceeds 2× the product's average daily complaint baseline *(trend proxy — see note below)*

| Assumption | Rationale |
|---|---|
| Target margin = 35% | Reasonable retail target; configurable via `TARGET_MARGIN_PCT` |
| Max single-step increase = 10% | Prevents aggressive automated price jumps that could damage customer trust |
| Competitor prices stored as a range string ("109–140"); midpoint used for positioning, ceiling caps increases | Midpoint represents market consensus; ceiling prevents pricing above market |
| Product names in the sentiment report are fuzzy-matched to catalog titles via `PRODUCT_ALIASES` | The LLM sometimes extracts "Blender" rather than "Portable Blender" |

**Note on "trending upward" (HC-2):**
The specification requires blocking increases when negative sentiment is *trending upward*, which strictly requires time-series data across multiple days. This prototype does not persist data between runs. As a documented approximation:
- We treat a complaint count > 2× the product's `avg_daily_complaints` baseline (from `pricing_context.csv`) as a rising-trend signal.
- In production, this would be replaced by a proper rolling N-day comparison stored in a database or time-series file.
- This assumption is explicitly noted in the `apply_pricing_logic` function's inline comments.

### Stage 2.4 — Daily Report

| Assumption | Rationale |
|---|---|
| Output format: self-contained HTML | More scannable than JSON or Markdown for a daily operations workflow; no server needed |
| "REQUIRE ACTION" badge counts only Complaints and Transactional Requests | Suggestions and Inquiries marked high-urgency are important but do not require immediate merchant intervention in the same way |
| High-urgency section capped at 20 unique messages | Enough to see the pattern; beyond 20, the merchant should use the classified CSV for a full review |
| The report is entirely offline | Google Fonts is the only external dependency and degrades gracefully if offline |

---

## Configurable Constants (quick reference)

All thresholds live at the top of each script so they can be adjusted without touching the logic.

| Constant | File | Default | Meaning |
|---|---|---|---|
| `MIN_MARGIN_PCT` | pricing / catalog | 0.15 | HC-1 gross margin floor |
| `TARGET_MARGIN_PCT` | pricing | 0.35 | Ideal margin to price toward |
| `NEG_SENTIMENT_BLOCK_PCT` | pricing | 0.30 | HC-2 negative-rate threshold |
| `COMPLAINT_BLOCK_THRESHOLD` | pricing | 3 | HC-2 daily complaint count threshold |
| `COMPLAINT_TREND_RATIO` | pricing | 2.0 | HC-2 trend proxy: block if today > ratio × daily avg |
| `MAX_SINGLE_INCREASE` | pricing | 0.10 | Max price increase per cycle (10%) |
| `COMPLAINT_RATE_ALERT` | sentiment | 0.30 | Overall complaint spike threshold |
| `REPEATED_MSG_THRESHOLD` | sentiment | 10 | Min occurrences to flag a message as a template |

---

*Salla Autonomous Merchant Operations Agent · Junior Developer Case Study*
