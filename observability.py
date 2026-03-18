"""
=============================================================================
 Observability — Salla Autonomous Merchant Operations Agent
 Deliverable 4: Basic tracing / logging
=============================================================================

 Provides a lightweight, zero-dependency structured logger that instruments
 every stage of the pipeline.  All trace events are:

   1. Written to  agent_trace.log  (JSONL — one event per line, easy to grep)
   2. Printed to  stdout           (human-readable, colour-coded by level)

 HOW TO USE (drop-in — add two lines to any pipeline script):

   from observability import tracer

   # Mark a stage start / end
   tracer.stage_start("2.1 catalog_analysis", inputs={"file": INPUT_PATH})
   tracer.stage_end(  "2.1 catalog_analysis", outputs={"unique_products": 8})

   # Log an LLM call
   tracer.llm_call(model="llama3.2:3b", prompt_chars=412,
                   result="Complaint", latency_ms=340, classifier="ollama")

   # Log an anomaly or alert
   tracer.alert(severity="high", alert_type="COMPLAINT_SPIKE",
                message="Complaint rate 43.7% exceeds 30% threshold")

   # Log a pricing decision
   tracer.pricing_decision(product="Portable Blender", action="BLOCKED",
                           reason="HC-2: 502 complaints >= 3 threshold")

   # Log any other event
   tracer.event("dedup", {"unique": 32, "total": 5000, "saved_llm_calls": 4968})

 OUTPUT EXAMPLE (agent_trace.log):
   {"ts":"2026-03-18T22:20:01","level":"INFO","stage":"2.1","event":"stage_start","inputs":{"file":"products_raw.csv"}}
   {"ts":"2026-03-18T22:20:02","level":"INFO","stage":"2.1","event":"stage_end","outputs":{"unique_products":8},"duration_s":1.2}
   {"ts":"2026-03-18T22:20:05","level":"INFO","stage":"2.2","event":"llm_call","model":"llama3.2:3b","result":"Complaint","latency_ms":340}
   {"ts":"2026-03-18T22:20:08","level":"WARN","stage":"2.2","event":"alert","severity":"high","alert_type":"COMPLAINT_SPIKE"}

 NOTE ON LANGSMITH:
   The task recommends LangSmith for tracing.  This module is a self-contained
   alternative that requires zero cloud credentials and works fully offline —
   matching the "runs 100% locally" design principle of the agent.
   If you want LangSmith instead, see the commented-out section at the bottom.
=============================================================================
"""

import json
import time
import datetime
import sys
from pathlib import Path
from typing import Any


# =============================================================================
# SECTION 0 — Configuration
# =============================================================================

LOG_FILE  = "agent_trace.log"   # JSONL output file (one event per line)
LOG_LEVEL = "DEBUG"             # DEBUG | INFO | WARN | ERROR

# ANSI colour codes (stdout only — not written to the log file)
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_COLORS = {
    "DEBUG": "\033[90m",   # dark grey
    "INFO":  "\033[36m",   # cyan
    "WARN":  "\033[33m",   # yellow
    "ERROR": "\033[31m",   # red
}

_LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3}


# =============================================================================
# SECTION 1 — Core tracer class
# =============================================================================

class AgentTracer:
    """
    Lightweight structured tracer for the Salla pipeline.

    Thread-safe for sequential pipelines (no locking needed).
    Each call appends a JSON line to LOG_FILE and prints to stdout.
    """

    def __init__(self, log_file: str = LOG_FILE, level: str = LOG_LEVEL):
        self.log_path    = Path(log_file)
        self.min_level   = _LEVEL_ORDER.get(level.upper(), 1)
        self._stage      = "init"
        self._stage_t0   = time.monotonic()
        self._run_id     = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self._llm_calls  = 0
        self._llm_total_ms = 0.0

        # Write a run-start header so each run is clearly delimited in the log
        self._write({
            "event":   "run_start",
            "run_id":  self._run_id,
            "level":   "INFO",
        })

    # ── internal helpers ─────────────────────────────────────────────────────

    def _now(self) -> str:
        return datetime.datetime.now().isoformat(timespec="seconds")

    def _write(self, payload: dict) -> None:
        """Append one JSONL line to the log file AND print to stdout."""
        record = {
            "ts":    self._now(),
            "stage": self._stage,
            **payload,
        }
        level = record.get("level", "INFO")

        # File output: raw JSON, no colour
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        # Stdout output: human-readable with colour
        if _LEVEL_ORDER.get(level, 1) >= self.min_level:
            colour  = _COLORS.get(level, "")
            icon    = {"INFO": "ℹ", "WARN": "⚠", "ERROR": "✖", "DEBUG": "·"}.get(level, "·")
            event   = record.get("event", "")
            details = {k: v for k, v in record.items()
                       if k not in ("ts", "stage", "level", "event", "run_id")}
            detail_str = "  " + "  ".join(f"{k}={v}" for k, v in details.items()) if details else ""
            tag = f"[{self._stage}]" if self._stage != "init" else "[agent]"
            print(f"  {colour}{icon} {tag} {event}{detail_str}{_RESET}")

    # ── public API ───────────────────────────────────────────────────────────

    def stage_start(self, stage_name: str, inputs: dict | None = None) -> None:
        """
        Call at the top of each pipeline stage.

        Example:
            tracer.stage_start("2.1 catalog_analysis",
                               inputs={"file": INPUT_PATH, "rows": len(raw)})
        """
        self._stage  = stage_name
        self._stage_t0 = time.monotonic()
        print(f"\n  {'─'*52}")
        print(f"  {_BOLD}▶  {stage_name}{_RESET}")
        print(f"  {'─'*52}")
        self._write({
            "event":  "stage_start",
            "level":  "INFO",
            "inputs": inputs or {},
        })

    def stage_end(self, stage_name: str, outputs: dict | None = None) -> None:
        """
        Call at the bottom of each pipeline stage.

        Example:
            tracer.stage_end("2.1 catalog_analysis",
                             outputs={"unique_products": 8, "issues_found": 5000})
        """
        duration = round(time.monotonic() - self._stage_t0, 2)
        self._write({
            "event":      "stage_end",
            "level":      "INFO",
            "outputs":    outputs or {},
            "duration_s": duration,
        })
        print(f"  {_COLORS['INFO']}✓  {stage_name} completed in {duration}s{_RESET}\n")

    def llm_call(self,
                 model:        str,
                 prompt_chars: int,
                 result:       str,
                 latency_ms:   float,
                 classifier:   str = "ollama",
                 message_id:   Any = None) -> None:
        """
        Log a single LLM classification call.

        Called once per unique message text in the dedup pipeline.
        Tracks running totals for the final summary.

        Example:
            tracer.llm_call(model="llama3.2:3b", prompt_chars=412,
                            result="Complaint", latency_ms=284, message_id=7)
        """
        self._llm_calls    += 1
        self._llm_total_ms += latency_ms
        payload: dict = {
            "event":       "llm_call",
            "level":       "DEBUG",
            "model":       model,
            "classifier":  classifier,
            "prompt_chars":prompt_chars,
            "result":      result,
            "latency_ms":  round(latency_ms, 1),
        }
        if message_id is not None:
            payload["message_id"] = message_id
        self._write(payload)

    def llm_fallback(self, message_id: Any = None, reason: str = "") -> None:
        """
        Log when Ollama is unreachable and the rule-based fallback fires.

        Example:
            tracer.llm_fallback(message_id=7, reason="Ollama connection refused")
        """
        self._write({
            "event":      "llm_fallback",
            "level":      "WARN",
            "classifier": "rule_based",
            "message_id": message_id,
            "reason":     reason,
        })

    def alert(self,
              severity:   str,
              alert_type: str,
              message:    str,
              **kwargs) -> None:
        """
        Log an anomaly alert raised by the sentiment pipeline.

        Example:
            tracer.alert(severity="high", alert_type="COMPLAINT_SPIKE",
                         message="Rate 43.7% exceeds 30%", complaints=2187)
        """
        level = "ERROR" if severity == "high" else "WARN"
        self._write({
            "event":      "alert",
            "level":      level,
            "severity":   severity,
            "alert_type": alert_type,
            "message":    message,
            **kwargs,
        })

    def pricing_decision(self,
                         product: str,
                         action:  str,
                         reason:  str,
                         current_price: float | None = None,
                         recommended_price: float | None = None) -> None:
        """
        Log a single pricing recommendation.

        Example:
            tracer.pricing_decision(
                product="Portable Blender", action="BLOCKED",
                reason="HC-2: 502 complaints >= 3",
                current_price=160.09)
        """
        level = "WARN" if action in ("BLOCKED", "DATA_INCOMPLETE") else "INFO"
        self._write({
            "event":             "pricing_decision",
            "level":             level,
            "product":           product,
            "action":            action,
            "reason":            reason,
            "current_price":     current_price,
            "recommended_price": recommended_price,
        })

    def event(self, name: str, data: dict | None = None, level: str = "INFO") -> None:
        """
        Log any custom event not covered by the typed helpers above.

        Example:
            tracer.event("dedup", {"unique": 32, "total": 5000})
        """
        self._write({
            "event": name,
            "level": level,
            **(data or {}),
        })

    def summary(self) -> None:
        """
        Print and log a run summary at the end of the full pipeline.
        Call once after all four stages complete.
        """
        total_s = round(time.monotonic() - self._stage_t0, 1)
        avg_ms  = (round(self._llm_total_ms / self._llm_calls, 1)
                   if self._llm_calls else 0)
        data = {
            "event":           "run_end",
            "level":           "INFO",
            "run_id":          self._run_id,
            "total_llm_calls": self._llm_calls,
            "avg_llm_ms":      avg_ms,
        }
        self._write(data)

        print(f"\n  {'═'*52}")
        print(f"  {_BOLD}Pipeline complete{_RESET}")
        print(f"  {'═'*52}")
        print(f"  {_COLORS['INFO']}Run ID       : {self._run_id}{_RESET}")
        print(f"  {_COLORS['INFO']}LLM calls    : {self._llm_calls}  (avg {avg_ms} ms each){_RESET}")
        print(f"  {_COLORS['INFO']}Trace log    : {self.log_path.resolve()}{_RESET}")
        print(f"  {'═'*52}\n")


# =============================================================================
# SECTION 2 — Module-level singleton (import and use immediately)
# =============================================================================

#  Every pipeline script imports this single object:
#      from observability import tracer
#
#  It writes to  agent_trace.log  in the working directory.
tracer = AgentTracer()


# =============================================================================
# SECTION 3 — Integration examples (for each pipeline script)
# =============================================================================
#
# ── catalog_analysis.py ───────────────────────────────────────────────────────
#
#   from observability import tracer
#
#   def run_catalog_analysis(...):
#       tracer.stage_start("2.1 catalog_analysis", {"file": input_path})
#       rows = load_csv(input_path)
#       tracer.event("load", {"rows": len(rows)})
#       cleaned = [clean_row(r) for r in rows]
#       dupe_groups = detect_duplicates(cleaned)
#       unique = deduplicate(cleaned, dupe_groups)
#       tracer.stage_end("2.1 catalog_analysis", {
#           "raw_rows": len(rows),
#           "unique_products": len(unique),
#           "issues_flagged": sum(1 for r in cleaned if r["_issues"]),
#           "duplicate_groups": len(dupe_groups),
#       })
#
# ── sentiment_analysis_ollama.py ──────────────────────────────────────────────
#
#   from observability import tracer
#
#   def classify_message(message):
#       t0 = time.monotonic()
#       result = classify_via_ollama(message)
#       if result is None:
#           tracer.llm_fallback(reason="Ollama unavailable")
#           result = classify_rule_based(message)
#       else:
#           tracer.llm_call(
#               model=OLLAMA_MODEL, prompt_chars=len(message),
#               result=result["category"], latency_ms=(time.monotonic()-t0)*1000)
#       return result
#
#   def run(...):
#       tracer.stage_start("2.2 sentiment_analysis", {"file": input_path})
#       ...
#       for a in alerts:
#           tracer.alert(severity=a["severity"], alert_type=a["type"],
#                        message=a["message"])
#       tracer.stage_end("2.2 sentiment_analysis", {
#           "total_messages": len(rows),
#           "unique_templates": total_unique,
#           "high_urgency": len(high_urgency),
#           "alerts": len(alerts),
#       })
#
# ── pricing_recommendations.py ────────────────────────────────────────────────
#
#   from observability import tracer
#
#   def run_pricing_pipeline(...):
#       tracer.stage_start("2.3 pricing", {"products": len(catalog)})
#       recs = [apply_pricing_logic(s) for s in signals_list]
#       for r in recs:
#           tracer.pricing_decision(
#               product=r.title, action=r.action,
#               reason=r.constraints_checked[-1] if r.constraints_checked else "",
#               current_price=r.current_price, recommended_price=r.recommended_price)
#       tracer.stage_end("2.3 pricing", {
#           "increase": sum(1 for r in recs if r.action=="INCREASE"),
#           "blocked":  sum(1 for r in recs if r.action=="BLOCKED"),
#           "hold":     sum(1 for r in recs if r.action=="HOLD"),
#       })
#
# ── daily_report_ollama.py ────────────────────────────────────────────────────
#
#   from observability import tracer
#
#   def main():
#       tracer.stage_start("2.4 daily_report", {"output": OUTPUT_PATH})
#       html = build_full_html(catalog, sentiment, pricing)
#       Path(OUTPUT_PATH).write_text(html)
#       tracer.stage_end("2.4 daily_report", {"output_bytes": len(html)})
#       tracer.summary()   # ← print run summary + write run_end event


# =============================================================================
# SECTION 4 — LangSmith alternative (commented out)
# =============================================================================
#
# If you prefer LangSmith tracing, replace the AgentTracer class above with:
#
#   pip install langsmith
#
#   import os
#   from langsmith import Client
#   from langsmith.run_trees import RunTree
#
#   os.environ["LANGCHAIN_TRACING_V2"] = "true"
#   os.environ["LANGCHAIN_API_KEY"]    = "<your-key>"
#   os.environ["LANGCHAIN_PROJECT"]    = "salla-merchant-agent"
#
#   ls_client = Client()
#
#   # Wrap each stage as a LangSmith run:
#   with RunTree(name="2.2 sentiment_analysis", run_type="chain") as run:
#       result = classify_message(msg)
#       run.end(outputs={"category": result["category"]})
#
# Everything else in this file (stage_start/end, llm_call, alert, etc.)
# maps 1-to-1 to LangSmith run inputs/outputs/metadata — no logic changes needed.
#
# This module was chosen over LangSmith because:
#   · Zero cloud credentials required — matches "runs 100% locally" design goal
#   · Zero pip installs — consistent with the rest of the project
#   · JSONL output is trivially grepable and parseable for post-run analysis


# =============================================================================
# SECTION 5 — CLI: replay / pretty-print a trace log
# =============================================================================
#
#   python observability.py agent_trace.log
#
# Prints every event in the log in a readable table.

if __name__ == "__main__":
    import sys

    log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(LOG_FILE)

    if not log_path.exists():
        print(f"No trace log found at {log_path}")
        sys.exit(0)

    events = [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]

    print(f"\n  Trace log: {log_path}  ({len(events)} events)\n")
    print(f"  {'TIME':<22} {'LEVEL':<6} {'STAGE':<28} {'EVENT':<22} DETAILS")
    print(f"  {'─'*100}")

    for e in events:
        ts      = e.get("ts", "")[-8:]           # show HH:MM:SS only
        level   = e.get("level", "INFO")
        stage   = e.get("stage", "")[:27]
        event   = e.get("event", "")[:21]
        details = {k: v for k, v in e.items()
                   if k not in ("ts", "level", "stage", "event", "run_id")}
        detail_str = "  ".join(f"{k}={v}" for k, v in list(details.items())[:3])
        colour  = _COLORS.get(level, "")
        print(f"  {colour}{ts:<22} {level:<6} {stage:<28} {event:<22} {detail_str}{_RESET}")

    print()
