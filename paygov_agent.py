# Databricks notebook source
pip install openai

# COMMAND ----------

import re

# COMMAND ----------

from openai import OpenAI
import pandas as pd
from pyspark.sql import functions as F

# === Connect to OpenRouter ===
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key="<KEY>",  # ⬅️ replace this with your key
)


# COMMAND ----------

spark.sql("USE CATALOG paygov")
spark.sql("USE SCHEMA hr_data")

def unadjusted_gap_gender(job_family="*", job_level="*", location="*"):
    df = spark.table("v_total_comp")
    if job_family != "*": df = df.filter(F.col("job_family")==job_family)
    if job_level  != "*": df = df.filter(F.col("job_level")==job_level)
    if location   != "*": df = df.filter(F.col("location")==location)
    return (df.groupBy("comp_year","gender")
              .agg(F.round(F.avg("total_comp"),2).alias("avg_total_comp"),
                   F.count("*").alias("employee_count"))
              .orderBy("comp_year","gender"))

def adjusted_gap_gender(job_family="*", location="*"):
    df = spark.table("v_with_tenure")
    if job_family != "*": df = df.filter(F.col("job_family")==job_family)
    if location   != "*": df = df.filter(F.col("location")==location)
    strata = (df.groupBy("comp_year","job_level","tenure_band","location")
              .agg(F.round(F.avg(F.when(F.col("gender")=="Female", F.col("total_comp"))),2).alias("avg_female"),
                   F.round(F.avg(F.when(F.col("gender")=="Male",   F.col("total_comp"))),2).alias("avg_male")))
    return (strata
            .withColumn("gender_gap_ratio",
                        F.when((F.col("avg_male")==0)|(F.col("avg_male").isNull()), F.lit(None))
                         .otherwise(F.round(F.col("avg_female")/F.col("avg_male"),4)))
            .orderBy("comp_year","job_level","tenure_band","location"))

def breakdown(by="tenure", job_family="*"):
    df = spark.table("v_with_tenure")
    if job_family != "*": df = df.filter(F.col("job_family")==job_family)
    key = {"tenure":"tenure_band","role":"job_level","location":"location"}.get(by,"tenure_band")
    return (df.groupBy(key,"comp_year")
              .agg(F.count("*").alias("employees"),
                   F.round(F.avg("total_comp"),2).alias("avg_total_comp"))
              .orderBy("comp_year", key))


# COMMAND ----------

# Expose ONLY governed tools; no raw SQL is ever offered.
TOOLS = {
    "unadjusted_gap_gender": {
        "fn": unadjusted_gap_gender,
        "args": {"job_family":"*", "job_level":"*", "location":"*"}
    },
    "adjusted_gap_gender": {
        "fn": adjusted_gap_gender,
        "args": {"job_family":"*", "location":"*"}
    },
    "breakdown": {
        "fn": breakdown,
        "args": {"by":"tenure", "job_family":"*"}  # by in ['tenure','role','location']
    }
}

PII_PATTERNS = [
    r"\bssn\b", r"social\s*security", r"\bemail\b", r"\bphone\b",
    r"\baddress\b", r"\bname\b", r"\bwho is\b", r"top paid employee"
]

def pii_guard(user_q: str) -> bool:
    q = user_q.lower()
    return any(re.search(p, q) for p in PII_PATTERNS)

def summarize_observation(df):
    # Only return safe aggregates summary (no raw rows with identifiers).
    # Limit to small preview counts and column stats.
    pdf = df.limit(50).toPandas()
    col_list = list(pdf.columns)
    rows = len(pdf)
    # simple rollups if present
    extra = {}
    for cand in ["avg_total_comp","employee_count","gender_gap_ratio"]:
        if cand in pdf.columns:
            try:
                extra[cand] = float(pd.to_numeric(pdf[cand], errors="coerce").mean())
            except Exception:
                pass
    return {
        "columns": col_list,
        "rows_previewed": rows,
        "sample": pdf.head(5).to_dict(orient="records"),
        "rollup": extra
    }


# COMMAND ----------

print(run_agent("What is the adjusted gender pay gap after controlling for level and tenure?"))


# COMMAND ----------

print(run_agent("What is the adjusted gender pay gap after controlling for level and tenure?"))


# COMMAND ----------

import json, re
from typing import Any, Dict

# --- leave your SYSTEM_PROMPT, ALLOWED_TOOLS, pii_guard, summarize_observation, TOOLS, and llm_json as they are ---

def _finalize_with_llm(question: str, observation: dict, model="mistralai/mistral-7b-instruct:free") -> str:
    """
    Ask the LLM to produce the final answer from the safe observation.
    If it fails, raise so we can fallback to a deterministic summary.
    """
    msg = [
        {"role":"system","content":"You write concise HR compliance summaries. No PII. Use only the provided aggregates."},
        {"role":"user","content":json.dumps({
            "question": question,
            "observation": observation  # columns, rows_previewed, sample, rollup
        })}
    ]
    resp = client.chat.completions.create(
        model=model,
        messages=msg,
        temperature=0,
        extra_headers={"HTTP-Referer":"https://databricks-community.ai","X-Title":"Pay-Equity-Copilot"},
    )
    content = (resp.choices[0].message.content or "").strip()
    if not content:
        raise ValueError("empty final")
    return content

def _fallback_final(question: str, observation: dict) -> str:
    """
    Deterministic summary when the LLM won’t cooperate.
    """
    roll = observation.get("rollup", {}) or {}
    parts = []
    if "gender_gap_ratio" in roll and roll["gender_gap_ratio"] is not None:
        parts.append(f"Avg female/male pay ratio (mean across strata) ≈ {roll['gender_gap_ratio']:.2f}.")
    if "avg_total_comp" in roll and roll["avg_total_comp"] is not None:
        parts.append(f"Average total compensation ≈ {roll['avg_total_comp']:.0f}.")
    if "employee_count" in roll and roll["employee_count"] is not None:
        parts.append(f"Average employee count in groups ≈ {roll['employee_count']:.1f}.")
    if not parts:
        parts.append("Aggregates computed successfully. No PII was accessed.")
    return " ".join(parts)

def run_agent(user_question: str, model="mistralai/mistral-7b-instruct:free", show_table=True):
    # 1) PII guard
    if pii_guard(user_question):
        return "I can’t provide PII or identify individuals. Please ask for aggregate analysis."

    # 2) Ask LLM for a tool plan (JSON), with retry already inside llm_json
    messages = [
        {"role":"system","content": SYSTEM_PROMPT},
        {"role":"user","content": user_question}
    ]
    try:
        plan = llm_json(messages, model=model, retry=True)
    except Exception:
        # Simple keyword fallback
        q = user_question.lower()
        if "adjusted" in q or "control" in q:
            plan = {"tool":"adjusted_gap_gender","args":{"job_family":"*","location":"*"}}
        elif "tenure" in q:
            plan = {"tool":"breakdown","args":{"by":"tenure","job_family":"*"}}
        elif "role" in q or "level" in q:
            plan = {"tool":"breakdown","args":{"by":"role","job_family":"*"}}
        elif "location" in q:
            plan = {"tool":"breakdown","args":{"by":"location","job_family":"*"}}
        else:
            plan = {"tool":"unadjusted_gap_gender","args":{"job_family":"*","job_level":"*","location":"*"}}

    # 3) Sanitize the plan (allowed tools + defaults)
    clean = sanitize_tool_call(plan)
    if "final" in clean:
        return clean["final"]
    if clean.get("error") == "unknown_tool":
        return "I couldn’t route that request to a supported analysis tool. Try 'adjusted', 'tenure', 'role', or 'location'."

    tool = clean["tool"]
    args = clean["args"]

    # 4) Execute the governed tool
    if tool == "unadjusted_gap_gender":
        df = TOOLS["unadjusted_gap_gender"]["fn"](**args)
    elif tool == "adjusted_gap_gender":
        df = TOOLS["adjusted_gap_gender"]["fn"](**args)
    elif tool == "breakdown":
        df = TOOLS["breakdown"]["fn"](**args)
    else:
        return "Unexpected tool routing error."

    if show_table:
        display(df)

    # 5) Build a safe observation summary to hand to the LLM
    obs = summarize_observation(df)

    # 6) Ask LLM to produce the final narrative; fallback deterministically if needed
    try:
        final_text = _finalize_with_llm(user_question, obs, model=model)
    except Exception:
        final_text = _fallback_final(user_question, obs)

    return final_text


# COMMAND ----------

print(run_agent("What is the adjusted gender pay gap after controlling for level and tenure?"))


# COMMAND ----------

print(run_agent("Break down compensation by tenure for Engineering."))


# COMMAND ----------

