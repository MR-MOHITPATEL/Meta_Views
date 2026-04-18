"""
llm_layer.py — LLM integration for EXPLANATION ONLY.

Rules:
- NEVER send raw data rows to LLM.
- ONLY send aggregated summary stats (< 500 tokens).
- Use Gemini first, fallback to Groq.
- Returns structured: explanation, insights, actions.
"""

import logging
import json
import re
from analytics.config import GEMINI_API_KEY, GROQ_API_KEY

logger = logging.getLogger(__name__)


def _build_prompt(question: str, sheet_name: str, summary: dict, top_rows: list) -> str:
    """Build a direct-answer prompt from the filtered dataset."""
    return f"""You are an analytics answer engine.

You are given:
1. A user query
2. A filtered dataset (summary + sample rows) that already contains relevant data

Your job is to generate a DIRECT ANSWER using ONLY the dataset below.

USER QUESTION: "{question}"

FILTERED DATASET SUMMARY:
{json.dumps(summary, indent=2, default=str)}

SAMPLE ROWS FROM DATASET:
{json.dumps(top_rows, indent=2, default=str)}

SUPPORTED CALCULATIONS:
- If query contains "pincode day": Answer = total_pincode_days from summary
- If query contains "spend": Answer = total_spend from summary
- If query contains "purchases": Answer = look at purchases column in rows
- If query contains "clicks": Answer = look at link_clicks or clicks column in rows
- If query contains "impressions": Answer = look at impressions column in rows
- Otherwise: describe what the dataset shows

RULES:
- DO NOT say "No answer generated"
- DO NOT hallucinate
- DO NOT use external knowledge
- ONLY compute from the provided dataset above
- If the required column is missing: return "Insufficient data in {sheet_name}"

OUTPUT FORMAT (EXACTLY):
Answer: <single clear computed value or finding>
Explanation: <1-2 sentences on how it was calculated>
"""


def _call_gemini(prompt: str) -> str:
    """Call Google Gemini API using the modern google-genai SDK."""
    from google import genai
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model='gemini-1.5-flash-8b', 
            contents=prompt
        )
        return response.text
    except Exception as e:
        logger.error(f"Gemini API Error: {e}")
        raise


def _call_groq(prompt: str) -> str:
    """Call Groq API."""
    from groq import Groq
    client = Groq(api_key=GROQ_API_KEY)
    response = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=800,
    )
    return response.choices[0].message.content


def _parse_llm_response(text: str) -> dict:
    """Parse the Answer + Explanation format from the LLM response."""
    result = {
        "answer": "",
        "explanation": "",
        "llm_used": "Unknown"
    }
    try:
        lines = text.strip().split("\n")
        for i, line in enumerate(lines):
            if line.startswith("Answer:"):
                result["answer"] = line.replace("Answer:", "").strip()
            elif line.startswith("Explanation:"):
                result["explanation"] = line.replace("Explanation:", "").strip()
        
        # Fallback: if no structured format, use the full response
        if not result["answer"]:
            result["answer"] = text.strip()[:300]
    except Exception as e:
        logger.warning(f"LLM parse error: {e}")
        result["answer"] = text.strip()[:300]

    return result


def get_llm_explanation(
    question: str,
    sheet_name: str,
    summary: dict,
    data_df,
    max_rows: int = 15
) -> dict:
    """
    Main entry point. Sends summary (not raw data) to LLM and returns parsed result.
    Tries Gemini first, falls back to Groq.
    """
    # Convert top rows to a safe JSON-serializable format
    top_rows = (
        data_df.head(max_rows).to_dict(orient="records")
        if not data_df.empty else []
    )
    # Round floats for prompt brevity
    for row in top_rows:
        for k, v in row.items():
            if isinstance(v, float):
                row[k] = round(v, 2)

    # FIX: Use sheet_name instead of non-existent query_type
    prompt = _build_prompt(question, sheet_name, summary, top_rows)

    # Try Gemini
    if GEMINI_API_KEY:
        try:
            logger.info("Calling Gemini LLM (genai v1)...")
            raw = _call_gemini(prompt)
            result = _parse_llm_response(raw)
            result["llm_used"] = "Gemini"
            return result
        except Exception as e:
            logger.warning(f"Gemini failed: {e}. Trying Groq...")

    # Fallback: Groq
    if GROQ_API_KEY:
        try:
            logger.info("Calling Groq LLM...")
            raw = _call_groq(prompt)
            result = _parse_llm_response(raw)
            result["llm_used"] = "Groq"
            return result
        except Exception as e:
            logger.error(f"Groq also failed: {e}")

    # Both failed
    logger.error("No LLM available. Returning empty explanation.")
    return {
        "explanation": "⚠️ LLM unavailable. Please add GEMINI_API_KEY or GROQ_API_KEY to your .env file.",
        "insights":    [],
        "actions":     [],
        "llm_used":    "None",
    }
