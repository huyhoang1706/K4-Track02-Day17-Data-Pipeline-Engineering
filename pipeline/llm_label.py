"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import json
import re

import duckdb

from .embed import text_hash

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Accept only a JSON object with exactly the declared label field."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict) or set(obj) != {"label"}:
        return None
    label = obj["label"]
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cache validated responses, including failures, by input/model/prompt version."""
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        raw_response VARCHAR, label VARCHAR,
        PRIMARY KEY (input_hash, model, prompt_version))""")
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_quarantine (
        ticket_id VARCHAR, input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        raw_response VARCHAR, reason VARCHAR,
        PRIMARY KEY (ticket_id, input_hash, model, prompt_version))""")
    rows, calls_before = [], llm.calls
    for ticket_id, text in live_tickets(con):
        prompt = PROMPT_TEMPLATE.format(text=text)
        # Hash the actual prompt too, so a template edit cannot reuse stale labels.
        key = (text_hash(prompt), llm.model, PROMPT_VERSION)
        cached = con.execute("""SELECT raw_response, label FROM llm_label_cache
            WHERE input_hash = ? AND model = ? AND prompt_version = ?""", key).fetchone()
        if cached is None:
            raw = llm.complete(prompt)
            label = parse_label(raw)
            con.execute("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?, ?)",
                        (*key, raw, label))
        else:
            raw, label = cached
        if label is None:
            con.execute("""INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING""",
                (ticket_id, *key, raw, 'Expected JSON label: bug / billing / other'))
        else:
            rows.append((ticket_id, label, llm.model, PROMPT_VERSION))
    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    if rows:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", rows)
    return {"labeled": len(rows), "calls": llm.calls - calls_before}
