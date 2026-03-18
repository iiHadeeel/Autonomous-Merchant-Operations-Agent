# Build Notes — Salla Merchant Operations Agent

---

## Why Ollama + llama3.2:3b?

The whole pipeline runs 100% locally — no API keys, no internet, no cloud costs. Ollama lets you run a real LLM on your own machine with one command (`ollama serve`). I picked `llama3.2:3b` because it's small enough to run fast on a laptop CPU but capable enough to reliably classify short customer messages into four categories.

If Ollama isn't running, the pipeline doesn't crash — it automatically falls back to a keyword-based classifier and keeps going.

---

## How the LLM is used

Every unique customer message gets sent to the model with a tight system prompt:

> *"Return ONLY a JSON object with category, sentiment, urgency, and product_mentioned."*

The model returns something like:
```json
{ "category": "Complaint", "sentiment": "negative", "urgency": "high", "product_mentioned": "Blender" }
```

Every field is validated against an allowed-values list before being used. If the model returns something unexpected, the rule-based fallback fills in the gap.

---

## The key optimisation: classify once, broadcast

5,000 messages sounds expensive. But in practice, customers copy-paste the same complaints — the sample data had only **32 unique message templates** across 5,000 rows.

So instead of 5,000 LLM calls, the pipeline:
1. Finds all unique messages (32)
2. Classifies each one once → 32 LLM calls
3. Copies the result to every matching row

That's a **99% reduction** in LLM calls with identical output.

---

## Design decisions

| Decision | Why |
|---|---|
| Sequential pipeline (4 scripts) | Each stage writes output to disk — easy to debug, resumable if one step fails |
| HTML report output | More scannable than JSON for a merchant checking their store at 6am |
| Stats computed over unique templates | Counting 500 copies of the same complaint as 500 separate signals inflates rates and breaks percentages |
| Fuzzy dedup for urgency messages | "where's my order??" and "where's my order????" are the same complaint — fingerprinting collapses them |
| HC-2 trend proxy | True trend detection needs multi-day history. Approximated as: block increase if today's complaints > 2× daily average |

---

## Assumptions

- A "duplicate" product = same normalised title + same price (different price = different SKU)
- `"unknown"` or `"??"` in the cost field = intentionally missing, not an error — flagged but kept
- The "REQUIRE ACTION" badge counts only Complaints and Transactional Requests — Inquiries and Suggestions don't require immediate merchant action
- Non-English descriptions are flagged but not removed — the merchant may be targeting multiple markets

---

