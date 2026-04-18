"""
Query parser: converts plain-English questions into structured intent dicts.

Pipeline:
  1. Gemini LLM — primary parser, handles complex/nuanced questions
  2. Semantic signal extraction — patches anything Gemini gets wrong
  3. Keyword fallback — fires when Gemini is unavailable / returns bad JSON
  4. Safe default — last resort, never returns an empty dict
"""

import json
import re
from datetime import date, datetime, timedelta

from dateutil import parser as dateutil_parser
from google import genai

from config import GEMINI_API_KEY, GEMINI_MODEL

_client = genai.Client(api_key=GEMINI_API_KEY)


# ════════════════════════════════════════════════════════════════════════════════
# DATE EXTRACTION
# ════════════════════════════════════════════════════════════════════════════════

_MONTHS = (
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)
_DATE_RE = re.compile(
    r"\d{1,2}(?:st|nd|rd|th)?\s+" + _MONTHS + r"(?:\s+\d{2,4})?"
    r"|" + _MONTHS + r"\s+\d{1,2}(?:st|nd|rd|th)?(?:\s+\d{2,4})?"
    r"|\d{4}-\d{2}-\d{2}"
    r"|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?",
    re.IGNORECASE,
)


def _parse_single_date(text: str, today: date) -> date | None:
    cleaned = re.sub(r"(\d+)(?:st|nd|rd|th)\b", r"\1", text, flags=re.IGNORECASE)
    try:
        d = dateutil_parser.parse(cleaned, default=datetime(today.year, 1, 1), dayfirst=True)
        if abs(d.year - today.year) > 1:
            d = d.replace(year=today.year)
        return d.date()
    except Exception:
        return None


def _extract_date_range(question: str, today: date) -> tuple[str | None, str | None, int | None]:
    q = question.lower()

    if m := re.search(r"last\s+(\d+)\s+day", q):
        return None, None, int(m.group(1))
    if m := re.search(r"last\s+(\d+)\s+week", q):
        return None, None, int(m.group(1)) * 7
    if m := re.search(r"last\s+(\d+)\s+month", q):
        return None, None, int(m.group(1)) * 30
    if "yesterday" in q:
        d = today - timedelta(days=1)
        return d.isoformat(), d.isoformat(), None
    if re.search(r"\btoday\b", q):
        return today.isoformat(), today.isoformat(), None
    if "this week" in q:
        start = today - timedelta(days=today.weekday())
        return start.isoformat(), today.isoformat(), None
    if "this month" in q:
        return today.replace(day=1).isoformat(), today.isoformat(), None

    spans = [m.group() for m in _DATE_RE.finditer(question)]
    if len(spans) >= 2:
        d1, d2 = _parse_single_date(spans[0], today), _parse_single_date(spans[1], today)
        if d1 and d2:
            lo, hi = min(d1, d2), max(d1, d2)
            return lo.isoformat(), hi.isoformat(), None
    if len(spans) == 1:
        d1 = _parse_single_date(spans[0], today)
        if d1:
            return d1.isoformat(), d1.isoformat(), None

    return None, None, None


# ════════════════════════════════════════════════════════════════════════════════
# THRESHOLD EXTRACTION
# ════════════════════════════════════════════════════════════════════════════════

_THRESHOLD_COL_MAP = {
    # CPT aliases (check multi-word first)
    "cost per transaction": "CPT", "cost per purchase": "CPT",
    "cpt": "CPT",
    # Other metrics
    "cpc": "CPC", "ctr": "CTR", "cvr": "CVR", "roas": "ROAS", "cpm": "CPM",
    "spend": "Spend",
    "purchases": "Purchases", "purchase": "Purchases", "sales": "Purchases",
    "clicks": "Clicks", "impressions": "Impressions", "revenue": "Revenue",
}


def _extract_thresholds(question: str) -> list[dict]:
    """
    Extract metric threshold conditions like:
      CPT<250, CPT < 250, purchases>2, ROAS >= 1.5
    Matches the last word (or two-word phrase) immediately before the operator.
    """
    thresholds = []
    op_map = {"<": "lt", "<=": "lte", ">": "gt", ">=": "gte", "=": "eq"}
    # Capture 1-3 words immediately before the operator
    pattern = r"(\b\w+(?:\s+\w+){0,2}?)\s*(<=|>=|<|>|=)\s*(\d+(?:\.\d+)?)"
    for m in re.finditer(pattern, question.lower()):
        col_raw = m.group(1).strip()
        # Take only the last 1-2 words in case the match captured extra context
        words = col_raw.split()
        for n in (2, 1):
            candidate = " ".join(words[-n:])
            col = _THRESHOLD_COL_MAP.get(candidate)
            if col:
                break
        else:
            continue
        thresholds.append({"column": col, "op": op_map[m.group(2)], "value": float(m.group(3))})
    return thresholds


# ════════════════════════════════════════════════════════════════════════════════
# SEMANTIC SIGNAL EXTRACTION
# Understands the question independently of exact wording.
# Works on top of both Gemini output and keyword fallback.
# ════════════════════════════════════════════════════════════════════════════════

# Intent signals — what the user wants to KNOW
_PINCODE_DAY_SIGNALS = re.compile(
    # Explicit "pincode day" / "pc day" mentions
    r"pincode.{0,5}day|pc.{0,5}day|pc days|pincode days|"
    # Hindi: "din" = day
    r"pincode.{0,10}din|pc.{0,10}din|kitne.{0,15}din|"
    # "how many active pincodes days" — 'days' anywhere near 'pincode'
    r"how many.{0,20}pincode.{0,10}day|"
    # "active PCs" / "active pc" — plural or singular
    r"active.{0,15}pcs?\b",
    re.I,
)
_PINCODE_COUNT_SIGNALS = re.compile(
    # Counting unique pincodes (NOT pincode-days, NOT daily breakdown, NOT ranking)
    r"how many pincodes?(?!\s*day|\s*din)|"
    r"unique.{0,10}pincode(?!.{0,30}creative)|pincode.{0,10}count|"
    # "which pincodes" ONLY for pure count/list — exclude daily, ranking, creative context
    r"which.{0,10}pincode(?!.{0,60}(?:each|per|every).{0,10}day|.{0,60}(?:generat|highest|most|best|rank|creative))|"
    r"list.{0,10}pincode(?!.{0,30}creative)|"
    r"kitne pincode(?!\s*day|\s*din)\b|"
    r"pincode list|"
    r"pincodes used(?!\s*(?:each|per|every).{0,10}day)|"
    r"pincodes active(?!\s*(?:each|per|every).{0,10}day)",
    re.I,
)
_WINNER_SIGNALS = re.compile(
    r"winner|winning|best.{0,15}creative|top.{0,15}creative|"
    r"healthy.{0,10}cpt|efficient.{0,10}cpt|low.{0,10}cpt|"
    r"best.{0,10}performing|highest.{0,10}roas|"
    r"lowest.{0,10}cpt|most.{0,10}purchase|"
    # "high purchases and efficient CPT" or "CPT below" + creative context
    r"high.{0,20}purchase.{0,40}cpt|cpt.{0,40}high.{0,20}purchase|"
    r"cpt.{0,20}(?:below|low|efficient|threshold)|"
    r"(?:below|above).{0,10}threshold.{0,40}creative|"
    r"creative.{0,40}(?:cpt|cost per).{0,20}(?:below|threshold|low)|"
    r"purchase.{0,20}(?:above|high|threshold)",
    re.I,
)
# Signals for daily pincode breakdown: Date × Pincode with spend/purchases
_DAILY_PINCODE_SIGNALS = re.compile(
    r"which.{0,20}pincode.{0,20}active.{0,20}(?:each|per|every).{0,10}day|"
    r"pincodes?.{0,20}active.{0,20}(?:each|per|every).{0,10}day|"
    r"daily.{0,20}pincode.{0,20}(?:usage|breakdown|performance|active|used)|"
    r"daily.{0,20}breakdown.{0,20}(?:of.{0,10})?(?:unique.{0,10})?pincode|"
    r"pincode.{0,20}(?:wise.{0,10})?daily.{0,20}(?:breakdown|performance|spend|usage)|"
    r"how.{0,15}(?:many|which).{0,20}pincode.{0,20}used.{0,20}(?:each|per).{0,10}day",
    re.I,
)
# Signals that mean "daily PC consumption" (kitne PC, how many PCs per day)
_DAILY_PC_SIGNALS = re.compile(
    r"kitne pc|how many pc.{0,10}consum|pc consumption|"
    r"pincode consumption|daily.{0,15}pc\b|daily.{0,15}pincode\b",
    re.I,
)
# Signals that mean "show me data grouped/trended by day" (not necessarily PC-specific)
_DAILY_SIGNALS = re.compile(
    r"\bdaily\b|day.{0,5}wise|day by day|per day|each day|"
    r"date.{0,5}wise|trend.{0,10}day|day.{0,5}trend",
    re.I,
)
_PC_WISE_SIGNALS = re.compile(
    r"pc.{0,5}wise|pincode.{0,5}wise|by pincode|per pincode|"
    r"pincode.{0,10}breakdown|pincode.{0,10}level",
    re.I,
)
_CREATIVE_SIGNALS = re.compile(
    r"creative.{0,5}wise|ad.{0,5}wise|by creative|per creative|"
    r"creative.{0,10}breakdown|which creative|each creative",
    re.I,
)
# "Pincode × Creative" lifetime questions — no date dimension
_PINCODE_CREATIVE_SIGNALS = re.compile(
    r"pincode.{0,30}(?:by|per|wise).{0,15}creative|"
    r"creative.{0,30}(?:by|per|in|for each).{0,15}pincode|"
    r"(?:best|top|highest|most).{0,20}pincode.{0,20}(?:purchase|spend|roas|cpt)|"
    r"(?:which|top|best).{0,20}pincode.{0,20}(?:generat|highest|most|best|rank|creative)|"
    r"best pincode.{0,20}(?:for|per|each)|"
    r"pincode.{0,20}(?:performance|rank|generat)|"
    r"pincode.{0,20}creative.{0,20}(?:performance|breakdown)|"
    r"(?:which pincode).{0,30}(?:most|highest|best)",
    re.I,
)
# Campaign-level questions
_CAMPAIGN_SIGNALS = re.compile(
    r"campaign.{0,5}wise|by campaign|per campaign|"
    r"campaign.{0,10}(?:performance|breakdown|level|spend|purchases|roas|cpt)|"
    r"which campaign|each campaign|across campaigns?",
    re.I,
)

# Metric signals — what number they want
_SPEND_SIGNALS    = re.compile(r"\bspend\b|spent|amount|cost\b", re.I)
_PURCHASE_SIGNALS = re.compile(r"\bpurchase|conversion|sale\b|result\b", re.I)
_CTR_SIGNALS      = re.compile(r"\bctr\b|click.{0,5}rate|click through", re.I)
_CPT_SIGNALS      = re.compile(r"\bcpt\b|cost per.{0,10}purchase|cost per.{0,10}transaction", re.I)
_ROAS_SIGNALS     = re.compile(r"\broas\b|return on ad", re.I)
_IMPRESSION_SIGNALS = re.compile(r"\bimpression|reach\b|view\b", re.I)
_CLICK_SIGNALS    = re.compile(r"\bclick(?!.{0,5}rate)|link click", re.I)

# Group/sort signals
_SORT_TOP_SIGNALS = re.compile(r"top\s+(\d+)|best\s+(\d+)|highest|most|largest", re.I)
_SORT_ASC_SIGNALS = re.compile(r"lowest|least|worst|smallest", re.I)

# Comparison: "compare X vs Y", "last week vs this week"
_COMPARISON_SIGNALS = re.compile(
    r"\bvs\.?\b|\bversus\b|compare.{0,30}(?:campaign|creative|week|month|period)|"
    r"(?:last|this).{0,10}week.{0,20}(?:vs|versus|compare|against).{0,20}(?:last|this|prev)|"
    r"(?:last|this).{0,10}month.{0,20}(?:vs|versus|compare|against)|"
    r"period.{0,10}(?:vs|versus|over|comparison)|week.{0,5}over.{0,5}week|"
    r"month.{0,5}over.{0,5}month|wow\b|mom\b",
    re.I,
)
# Adset-level questions
_ADSET_SIGNALS = re.compile(
    r"adset.{0,5}wise|ad.?set.{0,5}wise|by adset|by ad.?set|per adset|"
    r"adset.{0,10}(?:performance|breakdown|level|spend|best|worst)|"
    r"which adset|each adset|across adsets?|best.{0,10}adset",
    re.I,
)
# Overview / health check: "how is X doing", "overall performance", "is X profitable"
_OVERVIEW_SIGNALS = re.compile(
    r"how.{0,10}(?:is|are|was|were).{0,20}(?:campaign|creative|ad|doing|performing)|"
    r"overall.{0,10}performance|general.{0,10}performance|"
    r"(?:is|are).{0,20}(?:campaign|creative|ad).{0,20}(?:profitable|good|bad|healthy|performing)|"
    r"(?:what|how).{0,20}(?:performance|doing|going|results?)",
    re.I,
)


def _extract_limit(question: str) -> int | None:
    m = re.search(r"top\s+(\d+)|best\s+(\d+)|(\d+)\s+creative|(\d+)\s+ad|(\d+)\s+result", question, re.I)
    if m:
        return int(next(g for g in m.groups() if g))
    return None


def _extract_sort_col(question: str) -> tuple[str | None, str]:
    """
    Extract explicit sort column and direction.
    e.g. "sorted by CPT", "order by purchases desc", "ranked by ROAS"
    Returns (column_name, sort_order).
    """
    q = question.lower()
    m = re.search(
        r"(?:sort(?:ed)?|order(?:ed)?|rank(?:ed)?)\s+by\s+(\w+(?:\s+\w+)?)\s*(asc|desc|ascending|descending)?",
        q,
    )
    if m:
        col_raw = m.group(1).strip()
        direction = m.group(2) or "desc"
        sort_order = "asc" if direction.startswith("asc") else "desc"
        # Map to column name
        col = _THRESHOLD_COL_MAP.get(col_raw) or col_raw.capitalize()
        return col, sort_order
    return None, "desc"


def _extract_adset_filter(question: str) -> list[str] | None:
    """Extract adset names/numbers mentioned in the question."""
    _NOT_ID = {"wise", "has", "the", "with", "and", "for", "by", "in", "last",
               "this", "all", "performance", "breakdown", "level", "a", "an", "of"}
    raw = re.findall(r"ad.?set\s+([\w\-\.]+)", question, re.I)
    names = [n for n in raw if n.lower() not in _NOT_ID
             and not re.match(r"(has|have|is|are|was|were)\b", n, re.I)]
    return names if names else None


def _extract_creative_filter(question: str) -> str | None:
    """Extract specific creative name if user mentions one."""
    m = re.search(
        r"(?:for|of|creative|ad)\s+['\"]?([A-Z][A-Za-z0-9\s\-\.]+?)(?:['\"]|$|\s+(?:in|for|last|this|from))",
        question,
    )
    if m:
        name = m.group(1).strip()
        if len(name) > 3 and not re.match(r"(last|this|the|all|any)", name, re.I):
            return name
    return None


def _extract_campaign_filter(question: str) -> list[str] | None:
    """
    Extract one or more campaign names/identifiers from the question.

    Strategy:
      - "Campaign 35" or "campaign 36"           → match by short id "35", "36"
      - "Campaign 1 Interest Targeting Arjuna"   → match by FULL phrase so the
        aggregator can do an exact/substring match on the full campaign name
      - Multiple campaigns separated by commas/and are each captured

    Returns a list of strings; aggregator does case-insensitive substring match
    on Campaign name column for each entry (OR logic).
    """
    _NOT_ID = {
        "wise", "has", "the", "with", "and", "for", "by", "in", "last", "this",
        "all", "ad", "creative", "spend", "data", "performance", "breakdown",
        "level", "a", "an", "of", "which", "each", "name", "names",
    }
    # Terminators: words that end a campaign name phrase
    _END_PATTERN = re.compile(
        r"\b(and campaign|,\s*campaign|with ad|by ad|ad name|with date|"
        r"last \d+|this month|this week|yesterday|today|spend|purchase|"
        r"performance|breakdown|data|details)\b",
        re.I,
    )

    names: list[str] = []

    # Find every "Campaign <...>" occurrence in the question
    for m in re.finditer(r"campaign\s+(.+?)(?=,\s*campaign|\band\s+campaign\b|$)", question, re.I):
        raw = m.group(1).strip().rstrip(",. ")
        # Trim at common terminators
        end = _END_PATTERN.search(raw)
        if end:
            raw = raw[:end.start()].strip().rstrip(",. ")
        # Also trim trailing "and <number>" — that's a sibling campaign, not part of this name
        raw = re.sub(r"\s+and\s+\d+\s*$", "", raw, flags=re.I).strip()
        if not raw:
            continue
        # If it's just a single generic keyword or question fragment, skip
        if raw.lower() in _NOT_ID:
            continue
        # Skip if it looks like a question fragment (e.g. "has the highest spend")
        if re.match(r"(has|have|is|are|was|were)\b", raw, re.I):
            continue
        names.append(raw)

    if not names:
        return None

    # Also handle "campaign 35 and 36" — bare number after the last captured name
    last_m = None
    for last_m in re.finditer(r"campaign\s+[\w\s\-\.]+?(?=\band\b|\bor\b|,|$)", question, re.I):
        pass
    if last_m:
        tail = question[last_m.end():]
        extras = re.findall(r"(?:,|\band\b)\s*([\w\-\.]+)", tail, re.I)
        _SKIP = {"the", "with", "and", "for", "by", "in", "last", "this",
                 "all", "ad", "creative", "spend", "data", "performance", "name"}
        for e in extras:
            if e.lower() not in _SKIP and e not in names:
                names.append(e)

    return names if names else None


def _infer_metric(question: str) -> str:
    """Pick the most likely metric the user is asking about."""
    q = question.lower()
    if _CPT_SIGNALS.search(q):    return "CPT"
    if _ROAS_SIGNALS.search(q):   return "ROAS"
    if _CTR_SIGNALS.search(q):    return "CTR"
    if _PURCHASE_SIGNALS.search(q): return "Purchases"
    if _SPEND_SIGNALS.search(q):  return "Spend"
    if _IMPRESSION_SIGNALS.search(q): return "Impressions"
    if _CLICK_SIGNALS.search(q):  return "Clicks"
    return "Spend"


def _semantic_resolve(question: str, today: date) -> dict | None:
    """
    Classify query purely from semantic signals — no exact phrase matching.
    Returns a full intent dict, or None if signals are too weak.
    """
    q = question.lower()
    date_from, date_to, last_n = _extract_date_range(question, today)
    thresholds = _extract_thresholds(question)
    limit = _extract_limit(question)
    metric = _infer_metric(question)
    sort_order = "asc" if _SORT_ASC_SIGNALS.search(q) else "desc"

    campaigns = _extract_campaign_filter(question)
    adsets    = _extract_adset_filter(question)
    sort_col, sort_dir = _extract_sort_col(question)
    # sort_col from question overrides inferred sort_order
    if sort_col:
        sort_order = sort_dir
    filters = {
        "date_from": date_from, "date_to": date_to, "last_n_days": last_n,
        "campaign": None, "campaigns": campaigns,
        "adsets": adsets, "pincode": None,
        "creative": _extract_creative_filter(question),
        "thresholds": thresholds,
    }

    # ── Multi-dimension: Campaign + Adset + Creative ──────────────────────────
    # "campaign-wise along with adset and creative", "campaign adset creative breakdown"
    _has_campaign = bool(_CAMPAIGN_SIGNALS.search(q) or campaigns)
    _has_adset    = bool(_ADSET_SIGNALS.search(q))
    _has_creative = bool(_CREATIVE_SIGNALS.search(q) or re.search(r"creative|ad.?name", q, re.I))

    if _has_campaign and _has_adset and _has_creative:
        return {
            "dataset": "creative_performance", "intent": "breakdown",
            "metric": metric,
            "group_by": "Campaign name",
            "secondary_group_by": "Ad set name",
            "tertiary_group_by": "Ad name",
            "sort_by": sort_col or "Campaign name", "sort_order": sort_order, "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    if _has_campaign and _has_adset:
        return {
            "dataset": "creative_performance", "intent": "breakdown",
            "metric": metric,
            "group_by": "Campaign name",
            "secondary_group_by": "Ad set name",
            "tertiary_group_by": None,
            "sort_by": sort_col or metric, "sort_order": sort_order, "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    if _has_campaign and _has_creative:
        return {
            "dataset": "creative_performance", "intent": "breakdown",
            "metric": metric,
            "group_by": "Campaign name",
            "secondary_group_by": "Ad name",
            "tertiary_group_by": None,
            "sort_by": sort_col or metric, "sort_order": sort_order, "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    # ── Multi-campaign + ad name breakdown ────────────────────────────────────
    # "Campaign 35, 36, 1 with ad name" — show Date × Campaign name × Ad name
    if campaigns and (
        re.search(r"\bad.?name\b|by.{0,10}creative|by.{0,10}ad|with.{0,10}ad", question, re.I)
        or len(campaigns) > 1
        or re.search(r"\bshow\b|\bdata\b|\bdetails?\b|\bget\b", question, re.I)
    ):
        return {
            "dataset": "creative_performance", "intent": "breakdown",
            "metric": metric,
            "group_by": "Date",
            "secondary_group_by": "Ad name",
            "tertiary_group_by": None,
            "sort_by": "Date", "sort_order": "desc", "limit": limit,
            "query_type": "campaign_detail", "sub_intent": "count",
            "filters": filters,
        }

    # ── Comparison (vs / compare) ─────────────────────────────────────────────
    if _COMPARISON_SIGNALS.search(q):
        return {
            "dataset": "creative_performance", "intent": "comparison",
            "metric": metric, "group_by": "Ad name",
            "secondary_group_by": "Date",
            "sort_by": metric, "sort_order": sort_order, "limit": limit,
            "query_type": "comparison", "sub_intent": "count",
            "filters": filters,
        }

    # ── Adset-level questions ─────────────────────────────────────────────────
    if _ADSET_SIGNALS.search(q):
        has_campaign = bool(_CAMPAIGN_SIGNALS.search(q) or campaigns)
        has_creative = bool(_CREATIVE_SIGNALS.search(q) or re.search(r"creative|ad.?name|ad.?wise", q, re.I))
        if has_campaign:
            # Campaign + Adset + (optional Creative) — full 3-level drill-down
            return {
                "dataset": "creative_performance", "intent": "breakdown",
                "metric": metric,
                "group_by": "Campaign name",
                "secondary_group_by": "Ad set name",
                "tertiary_group_by": "Ad name" if has_creative else None,
                "sort_by": sort_col or metric, "sort_order": sort_order, "limit": limit,
                "query_type": "generic", "sub_intent": "count",
                "filters": filters,
            }
        return {
            "dataset": "creative_performance", "intent": "breakdown",
            "metric": metric,
            "group_by": "Ad set name",
            "secondary_group_by": "Ad name",
            "tertiary_group_by": None,
            "sort_by": sort_col or metric, "sort_order": sort_order, "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    # ── Overview / health check ───────────────────────────────────────────────
    if _OVERVIEW_SIGNALS.search(q):
        gb = "Campaign name" if (campaigns or _CAMPAIGN_SIGNALS.search(q)) else "Ad name"
        return {
            "dataset": "creative_performance", "intent": "breakdown",
            "metric": "all", "group_by": gb,
            "secondary_group_by": None, "tertiary_group_by": None,
            "sort_by": "Spend", "sort_order": "desc", "limit": limit,
            "query_type": "overview", "sub_intent": "count",
            "filters": filters,
        }

    # ── Campaign-level breakdown ───────────────────────────────────────────────
    if _CAMPAIGN_SIGNALS.search(q):
        daywise      = bool(_DAILY_SIGNALS.search(q))
        has_adset    = bool(_ADSET_SIGNALS.search(q))
        has_creative = bool(_CREATIVE_SIGNALS.search(q) or re.search(r"creative|ad.?name", q, re.I))

        # If adset or creative also requested → use creative_performance (has all columns)
        if has_adset or has_creative:
            return {
                "dataset": "creative_performance", "intent": "breakdown",
                "metric": metric,
                "group_by": "Campaign name",
                "secondary_group_by": "Ad set name" if has_adset else "Ad name",
                "tertiary_group_by": "Ad name" if (has_adset and has_creative) else None,
                "sort_by": sort_col or metric, "sort_order": sort_order, "limit": limit,
                "query_type": "generic", "sub_intent": "count",
                "filters": filters,
            }

        # Pure campaign breakdown
        return {
            "dataset": "campaign_performance" if not daywise else "creative_performance",
            "intent": "breakdown",
            "metric": metric,
            "group_by": "Date" if daywise else "Campaign name",
            "secondary_group_by": "Campaign name" if daywise else None,
            "tertiary_group_by": None,
            "sort_by": "Date" if daywise else metric,
            "sort_order": "desc" if daywise else sort_order,
            "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    # ── Pincode × Creative lifetime breakdown ──────────────────────────────────
    if _PINCODE_CREATIVE_SIGNALS.search(q):
        return {
            "dataset": "pincode_creative", "intent": "breakdown",
            "metric": metric, "group_by": "Pincode",
            "secondary_group_by": "Ad name",
            "sort_by": metric, "sort_order": sort_order, "limit": limit,
            "query_type": "pc_wise", "sub_intent": "count",
            "filters": filters,
        }

    # ── Daily pincode breakdown (Date × Pincode with spend/purchases) ────────────
    # Must check BEFORE pincode_count to avoid "which pincodes active each day"
    # being swallowed by the count signal.
    if _DAILY_PINCODE_SIGNALS.search(q):
        return {
            "dataset": "pc_creative_date", "intent": "breakdown",
            "metric": "Spend", "group_by": "Date",
            "secondary_group_by": "Pincode",
            "sort_by": "Date", "sort_order": "desc", "limit": limit,
            "query_type": "daily_pincode", "sub_intent": "count",
            "filters": filters,
        }

    # ── Pincode unique count / list (check BEFORE pincode-day to avoid overlap) ─
    # "which pincodes", "how many pincodes", "list pincodes" = count/list unique pincodes
    if _PINCODE_COUNT_SIGNALS.search(q):
        sub = "list" if re.search(r"\bwhich\b|\blist\b", q) else "count"
        return {
            "dataset": "pc_creative_date", "intent": "total",
            "metric": "Pincode", "group_by": "Date",
            "secondary_group_by": "Pincode",
            "sort_by": "Date", "sort_order": "desc", "limit": None,
            "query_type": "pincode_count", "sub_intent": sub,
            "filters": filters,
        }

    # ── Pincode day count (total active pc-days) ───────────────────────────────
    # "how many pincode days", "active pc days", "total pc days" = day-count metric
    if _PINCODE_DAY_SIGNALS.search(q):
        return {
            "dataset": "pc_creative_date", "intent": "total",
            "metric": "Pincode Days", "group_by": "Ad name",
            "secondary_group_by": "Date",
            "sort_by": "Pincode Days", "sort_order": "desc", "limit": limit,
            "query_type": "creative_pc_days", "sub_intent": "count",
            "filters": filters,
        }

    # ── Winners: only fire for explicit winner/best/top-N creative intent ──────
    # Avoid matching "which campaign has highest ROAS" (campaign-level, not winner)
    if _WINNER_SIGNALS.search(q) and not re.search(r"which.{0,15}campaign|by campaign|campaign.{0,10}wise", q, re.I):
        has_date = date_from or date_to or last_n
        # winning_creatives view has NO Date column — use creative_performance when date filter given
        dataset = "creative_performance" if has_date else "winning_creatives"
        return {
            "dataset": dataset, "intent": "list",
            "metric": "all", "group_by": "Ad name",
            "secondary_group_by": None,
            "sort_by": "CPT", "sort_order": "asc", "limit": limit or 20,
            "query_type": "winners", "sub_intent": "count",
            "filters": filters,
        }

    # ── Daily PC consumption (how many PCs per day) ────────────────────────────
    if _DAILY_PC_SIGNALS.search(q):
        return {
            "dataset": "daily_pc_consumption", "intent": "breakdown",
            "metric": "Pincode Days", "group_by": "Date",
            "secondary_group_by": "Ad name",
            "sort_by": "Date", "sort_order": "desc", "limit": limit,
            "query_type": "daily_consumption", "sub_intent": "count",
            "filters": filters,
        }

    # ── Daily trend of a metric (spend/ROAS/etc over time) ────────────────────
    if _DAILY_SIGNALS.search(q) and not _CREATIVE_SIGNALS.search(q):
        return {
            "dataset": "creative_performance", "intent": "trend",
            "metric": metric, "group_by": "Date",
            "secondary_group_by": None,
            "sort_by": "Date", "sort_order": "asc", "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    # ── PC-wise ────────────────────────────────────────────────────────────────
    if _PC_WISE_SIGNALS.search(q):
        return {
            "dataset": "pc_creative_date", "intent": "breakdown",
            "metric": metric, "group_by": "Pincode",
            "secondary_group_by": "Ad name",
            "sort_by": metric, "sort_order": sort_order, "limit": limit,
            "query_type": "pc_wise", "sub_intent": "count",
            "filters": filters,
        }

    # ── Creative-wise (spend / purchases / ROAS / CPT by creative) ────────────
    if _CREATIVE_SIGNALS.search(q) or re.search(r"by creative|per creative|each creative", q, re.I):
        daywise = bool(_DAILY_SIGNALS.search(q))
        return {
            "dataset": "creative_performance", "intent": "breakdown",
            "metric": metric,
            # When daywise, group Date first so table is sorted by Date then Ad name
            "group_by": "Date" if daywise else "Ad name",
            "secondary_group_by": "Ad name" if daywise else None,
            "sort_by": "Date" if daywise else metric,
            "sort_order": "desc" if daywise else sort_order,
            "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    # ── Single metric total or trend (spend last 7 days, daily spend trend) ───
    if _DAILY_SIGNALS.search(q):
        return {
            "dataset": "creative_performance", "intent": "trend",
            "metric": metric, "group_by": "Date",
            "secondary_group_by": None,
            "sort_by": "Date", "sort_order": "asc", "limit": limit,
            "query_type": "generic", "sub_intent": "count",
            "filters": filters,
        }

    # Weak signal — let Gemini handle it
    return None


# ════════════════════════════════════════════════════════════════════════════════
# GEMINI PROMPT
# ════════════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """
You are a query-parsing engine for a Meta Ads campaign analytics system.
Today's date: {today}

## DATASETS
1. creative_performance  – Date + Campaign name + Ad set name + Ad name: Spend, Impressions,
                           Clicks, Purchases, Revenue, CTR, CPC, CPT, CVR, ROAS, pincode_day
2. pc_creative_date      – Date + Pincode + Ad name: same metrics + pincode_day
3. daily_pc_consumption  – Date only: daily totals with pincode_day
4. winning_creatives     – Ad name only: lifetime CPT, Purchases, ROAS (no date)
5. pincode_creative      – Pincode + Ad name: lifetime totals
6. campaign_performance  – Date + Campaign name: daily campaign totals
7. raw_dump              – raw rows

## DATASETS — IMPORTANT
creative_performance is the MOST COMPLETE dataset. It has ALL dimensions:
  Date, Campaign name, Ad set name, Ad name + all metrics.
Use it whenever the user asks for 2+ of: campaign / adset / creative / ad name.
NEVER use campaign_performance when adset or creative is also requested.

## QUERY TYPE MAP
| Intent                                            | query_type        | dataset              | group_by → secondary → tertiary        |
|---------------------------------------------------|-------------------|----------------------|----------------------------------------|
| creative × pincode days / active pc days          | creative_pc_days  | pc_creative_date     | Ad name                                |
| pincode wise breakdown (with date)                | pc_wise           | pc_creative_date     | Pincode → Ad name                      |
| pincode × creative ranking                        | pc_wise           | pincode_creative     | Pincode → Ad name                      |
| how many/which pincodes used                      | pincode_count     | pc_creative_date     | Date → Pincode                         |
| daily pincode usage / pincodes each day           | daily_pincode     | pc_creative_date     | Date → Pincode                         |
| daily consumption / kitne pc                      | daily_consumption | daily_pc_consumption | Date → Ad name                         |
| winners / best creative / lowest CPT              | winners           | winning_creatives    | Ad name                                |
| compare X vs Y / last week vs this week           | comparison        | creative_performance | Ad name                                |
| how is X doing / overall performance              | overview          | creative_performance | Campaign name or Ad name               |
| adset wise only                                   | generic           | creative_performance | Ad set name → Ad name                  |
| campaign + adset wise                             | generic           | creative_performance | Campaign name → Ad set name            |
| campaign + creative/ad wise                       | generic           | creative_performance | Campaign name → Ad name                |
| campaign + adset + creative (ALL THREE)           | generic           | creative_performance | Campaign name → Ad set name → Ad name  |
| campaign wise only (no adset/creative)            | generic           | campaign_performance | Campaign name                          |
| spend/CTR/ROAS by creative                        | generic           | creative_performance | Ad name                                |

## EXAMPLES
Q: "campaign-wise along with adset name and creative wise performance"
→ query_type=generic, dataset=creative_performance, group_by=Campaign name, secondary_group_by=Ad set name, tertiary_group_by=Ad name

Q: "give me campaign and adset performance"
→ query_type=generic, dataset=creative_performance, group_by=Campaign name, secondary_group_by=Ad set name

Q: "compare last week vs this week"
→ query_type=comparison, dataset=creative_performance, intent=comparison, group_by=Date

Q: "how is Campaign 35 doing?"
→ query_type=overview, dataset=creative_performance, group_by=Campaign name, metric=all

Q: "show spend and purchases for campaign 35 by creative sorted by CPT"
→ query_type=generic, dataset=creative_performance, group_by=Ad name, sort_by=CPT, sort_order=asc

Q: "which adset is performing best?"
→ query_type=generic, dataset=creative_performance, group_by=Ad set name, metric=ROAS, sort_order=desc

Q: "top 5 ads by purchases last 30 days sorted by ROAS desc"
→ query_type=generic, dataset=creative_performance, group_by=Ad name, metric=Purchases, limit=5, sort_by=ROAS, sort_order=desc, last_n_days=30

Q: "is campaign 1 profitable?"
→ query_type=overview, dataset=creative_performance, group_by=Campaign name, metric=ROAS

Q: "compare campaign 35 vs campaign 36"
→ query_type=comparison, dataset=creative_performance, group_by=Campaign name

Q: "daily spend trend last 7 days"
→ query_type=generic, dataset=creative_performance, group_by=Date, metric=Spend, intent=trend, last_n_days=7

Q: "best performing creative last month"
→ query_type=winners, dataset=winning_creatives, sort_by=ROAS, sort_order=desc

Q: "pincode days by creative last 30 days"
→ query_type=creative_pc_days, dataset=pc_creative_date, metric=Pincode Days, last_n_days=30

## DATE RULES
- "last N days" → last_n_days=N (not date_from/date_to)
- "this week" → date_from={this_week_start}, date_to={today}
- "last week" → date_from={last_week_start}, date_to={last_week_end}
- "this month" → date_from={year}-{month}-01, date_to={today}
- "10th april" → {year}-04-10
- For comparisons: set comparison_period_1 and comparison_period_2 in filters if two periods mentioned

## THRESHOLD RULES
- "CPT<250 and purchases>2" → thresholds=[{{"column":"CPT","op":"lt","value":250}},{{"column":"Purchases","op":"gt","value":2}}]

## OUTPUT — ONLY valid JSON, no markdown, no explanation:
{{
  "dataset": "<dataset_key>",
  "intent": "<total|breakdown|trend|list|comparison|overview>",
  "metric": "<Spend|Purchases|Clicks|Impressions|Pincode Days|CTR|CPC|CPT|CVR|ROAS|Pincode|all>",
  "group_by": "<Ad name|Ad set name|Date|Pincode|Campaign name|null>",
  "secondary_group_by": "<Ad name|Ad set name|Date|Pincode|Campaign name|null>",
  "tertiary_group_by": "<Ad name|Ad set name|null>",
  "filters": {{
    "date_from": "<YYYY-MM-DD or null>",
    "date_to":   "<YYYY-MM-DD or null>",
    "last_n_days": <integer or null>,
    "campaign":  "<name or null>",
    "pincode":   "<value or null>",
    "creative":  "<name or null>",
    "thresholds": [],
    "comparison_period_1": {{"date_from": "<or null>", "date_to": "<or null>", "last_n_days": <or null>}},
    "comparison_period_2": {{"date_from": "<or null>", "date_to": "<or null>", "last_n_days": <or null>}}
  }},
  "sort_by":    "<column or null>",
  "sort_order": "<desc|asc>",
  "limit":      <integer or null>,
  "query_type": "<creative_pc_days|pc_wise|daily_consumption|winners|pincode_count|daily_pincode|comparison|overview|generic>",
  "sub_intent": "<count|list>"
}}
"""


# ════════════════════════════════════════════════════════════════════════════════
# KEYWORD FALLBACK  (fires only when Gemini AND semantic both fail)
# ════════════════════════════════════════════════════════════════════════════════

_KEYWORD_RULES = [
    (r"campaign.{0,5}wise|by campaign|per campaign|which campaign|"
     r"campaign.{0,10}(?:performance|breakdown|spend|purchases|roas)", {
        "dataset": "campaign_performance", "intent": "breakdown", "metric": "Spend",
        "group_by": "Campaign name", "secondary_group_by": None,
        "sort_by": "Spend", "sort_order": "desc", "limit": None,
        "query_type": "generic", "sub_intent": "count",
    }),
    (r"(?:top|best|highest|which).{0,20}pincode.{0,30}(?:purchase|spend|roas|creative|generat|rank)|"
     r"pincode.{0,20}(?:by|per|wise).{0,15}creative|"
     r"creative.{0,20}(?:by|in|per).{0,15}pincode", {
        "dataset": "pincode_creative", "intent": "breakdown", "metric": "Purchases",
        "group_by": "Pincode", "secondary_group_by": "Ad name",
        "sort_by": "Purchases", "sort_order": "desc", "limit": None,
        "query_type": "pc_wise", "sub_intent": "count",
    }),
    (r"daily.{0,20}pincode.{0,20}(?:usage|breakdown|performance)|"
     r"pincode.{0,20}active.{0,20}(?:each|per|every).{0,10}day|"
     r"daily.{0,20}breakdown.{0,20}pincode", {
        "dataset": "pc_creative_date", "intent": "breakdown", "metric": "Spend",
        "group_by": "Date", "secondary_group_by": "Pincode",
        "sort_by": "Date", "sort_order": "desc", "limit": None,
        "query_type": "daily_pincode", "sub_intent": "count",
    }),
    (r"winner|winning|healthy.?cpt|top.?creative|best.?creative|"
     r"efficient.?cpt|low.?cpt|cpt.{0,20}(?:below|threshold)|"
     r"(?:below|above).{0,10}threshold|high.{0,20}purchase.{0,30}creative|"
     r"creative.{0,30}cpt", {
        # dataset is patched below based on whether a date filter is present
        "dataset": "winning_creatives", "intent": "list", "metric": "all",
        "group_by": "Ad name", "secondary_group_by": None,
        "sort_by": "CPT", "sort_order": "asc", "limit": 20,
        "query_type": "winners", "sub_intent": "count",
    }),
    (r"pincode.{0,8}day|pc.{0,8}day|active.{0,10}pc|active.{0,10}pincode", {
        "dataset": "pc_creative_date", "intent": "breakdown", "metric": "Pincode Days",
        "group_by": "Ad name", "secondary_group_by": "Date",
        "sort_by": "Pincode Days", "sort_order": "desc", "limit": None,
        "query_type": "creative_pc_days", "sub_intent": "count",
    }),
    (r"how many.{0,20}pincode|unique.{0,10}pincode|which.{0,10}pincode|"
     r"pincode.{0,10}used|pincode.{0,10}active|kitne.{0,10}pincode|pincode list", {
        "dataset": "pc_creative_date", "intent": "total", "metric": "Pincode",
        "group_by": "Date", "secondary_group_by": "Pincode",
        "sort_by": "Date", "sort_order": "desc", "limit": None,
        "query_type": "pincode_count", "sub_intent": "count",
    }),
    (r"daily|kitne.?pc|pc.?consum|pincode.?consum|day.?wise|per day", {
        "dataset": "daily_pc_consumption", "intent": "breakdown", "metric": "Pincode Days",
        "group_by": "Date", "secondary_group_by": "Ad name",
        "sort_by": "Date", "sort_order": "desc", "limit": None,
        "query_type": "daily_consumption", "sub_intent": "count",
    }),
    (r"pc.?wise|pincode.?wise|by.?pincode|per.?pincode", {
        "dataset": "pc_creative_date", "intent": "breakdown", "metric": "Spend",
        "group_by": "Pincode", "secondary_group_by": "Ad name",
        "sort_by": "Spend", "sort_order": "desc", "limit": None,
        "query_type": "pc_wise", "sub_intent": "count",
    }),
    (r"creative.?wise|by.?creative|per.?creative|ad.?wise", {
        "dataset": "creative_performance", "intent": "breakdown", "metric": "Spend",
        "group_by": "Ad name", "secondary_group_by": None,
        "sort_by": "Spend", "sort_order": "desc", "limit": None,
        "query_type": "generic", "sub_intent": "count",
    }),
    (r"raw.?dump|raw.?data|transaction.?level", {
        "dataset": "raw_dump", "intent": "breakdown", "metric": "Spend",
        "group_by": "Ad name", "secondary_group_by": None,
        "sort_by": "Spend", "sort_order": "desc", "limit": None,
        "query_type": "generic", "sub_intent": "count",
    }),
]


def _keyword_fallback(question: str, today: date) -> dict | None:
    q = question.lower()
    for pattern, base in _KEYWORD_RULES:
        if re.search(pattern, q):
            intent = dict(base)
            date_from, date_to, last_n = _extract_date_range(question, today)
            intent["filters"] = {
                "date_from": date_from, "date_to": date_to, "last_n_days": last_n,
                "campaign": None, "pincode": None,
                "creative": _extract_creative_filter(question),
                "thresholds": _extract_thresholds(question),
            }
            # Winners with date filter → must use creative_performance (has Date column)
            if intent.get("query_type") == "winners" and (date_from or date_to or last_n):
                intent["dataset"] = "creative_performance"
            return intent
    return None


# ════════════════════════════════════════════════════════════════════════════════
# VALIDATION — fix common Gemini mistakes
# ════════════════════════════════════════════════════════════════════════════════

def _validate_and_fix(intent: dict, question: str, today: date) -> dict:
    """
    Post-process Gemini output.
    Fixes: wrong dataset for query_type, generic fallthrough on pincode questions,
    missing dates, missing sub_intent.
    """
    q = question.lower()

    # Fix: query_type=generic but question is clearly about something specific
    if intent.get("query_type") == "generic":
        if _COMPARISON_SIGNALS.search(q):
            intent["query_type"] = "comparison"
            intent["dataset"]    = "creative_performance"
        elif _ADSET_SIGNALS.search(q):
            has_camp = bool(_CAMPAIGN_SIGNALS.search(q))
            has_cre  = bool(_CREATIVE_SIGNALS.search(q) or re.search(r"creative|ad.?name", q, re.I))
            intent["dataset"] = "creative_performance"
            if has_camp:
                intent["group_by"]           = "Campaign name"
                intent["secondary_group_by"] = "Ad set name"
                intent["tertiary_group_by"]  = "Ad name" if has_cre else None
            else:
                intent["group_by"]           = "Ad set name"
                intent["secondary_group_by"] = "Ad name"
                intent["tertiary_group_by"]  = None
        elif _OVERVIEW_SIGNALS.search(q):
            intent["query_type"] = "overview"
            intent["dataset"]    = "creative_performance"
        elif _CAMPAIGN_SIGNALS.search(q):
            has_adset = bool(_ADSET_SIGNALS.search(q))
            has_cre   = bool(_CREATIVE_SIGNALS.search(q) or re.search(r"creative|ad.?name", q, re.I))
            if has_adset or has_cre:
                intent["dataset"]            = "creative_performance"
                intent["group_by"]           = "Campaign name"
                intent["secondary_group_by"] = "Ad set name" if has_adset else "Ad name"
                intent["tertiary_group_by"]  = "Ad name" if (has_adset and has_cre) else None
            elif intent.get("dataset") != "campaign_performance":
                intent["dataset"]  = "campaign_performance"
                intent["group_by"] = intent.get("group_by") or "Campaign name"
        elif _PINCODE_CREATIVE_SIGNALS.search(q):
            intent["dataset"]    = "pincode_creative"
            intent["group_by"]   = "Pincode"
            intent["secondary_group_by"] = "Ad name"
        elif _DAILY_PINCODE_SIGNALS.search(q):
            intent["query_type"] = "daily_pincode"
            intent["dataset"]    = "pc_creative_date"
            intent["group_by"]   = "Date"
            intent["secondary_group_by"] = "Pincode"
        elif _PINCODE_DAY_SIGNALS.search(q):
            intent["query_type"] = "creative_pc_days"
            intent["dataset"]    = "pc_creative_date"
            intent["metric"]     = "Pincode Days"
        elif _PINCODE_COUNT_SIGNALS.search(q):
            intent["query_type"] = "pincode_count"
            intent["dataset"]    = "pc_creative_date"
        elif _WINNER_SIGNALS.search(q):
            intent["query_type"] = "winners"
            intent["dataset"]    = "winning_creatives"
        elif _DAILY_SIGNALS.search(q) and not _CREATIVE_SIGNALS.search(q):
            intent["query_type"] = "daily_consumption"
            intent["dataset"]    = "daily_pc_consumption"

    # Fix: dataset/query_type mismatch
    _DATASET_FOR_QT = {
        "creative_pc_days":  "pc_creative_date",
        "pc_wise":           "pc_creative_date",
        "pincode_count":     "pc_creative_date",
        "daily_pincode":     "pc_creative_date",
        "daily_consumption": "daily_pc_consumption",
    }
    if intent.get("query_type") in _DATASET_FOR_QT:
        intent["dataset"] = _DATASET_FOR_QT[intent["query_type"]]

    # Winners: winning_creatives has NO Date column.
    # If a date filter is present, must use creative_performance instead.
    if intent.get("query_type") == "winners":
        f = intent.get("filters", {})
        has_date = f.get("last_n_days") or f.get("date_from") or f.get("date_to")
        intent["dataset"] = "creative_performance" if has_date else "winning_creatives"

    # Fix: limit extracted from question overrides Gemini if Gemini missed it
    if not intent.get("limit"):
        intent["limit"] = _extract_limit(question)

    # Fix: sub_intent default
    intent.setdefault("sub_intent", "count")
    if re.search(r"\bwhich\b|\blist\b", q):
        intent["sub_intent"] = "list"

    return intent


def _build_filters(f: dict, question: str, today: date) -> dict:
    f.setdefault("campaign", None)
    f.setdefault("campaigns", None)
    f.setdefault("adsets", None)
    f.setdefault("pincode", None)
    f.setdefault("creative", None)
    if not f.get("thresholds"):
        f["thresholds"] = _extract_thresholds(question)
    if not f.get("date_from") and not f.get("date_to") and not f.get("last_n_days"):
        df, dt, ln = _extract_date_range(question, today)
        f["date_from"], f["date_to"], f["last_n_days"] = df, dt, ln
    # Always re-extract campaigns/adsets since Gemini never returns them
    if not f.get("campaigns"):
        f["campaigns"] = _extract_campaign_filter(question)
    if not f.get("adsets"):
        f["adsets"] = _extract_adset_filter(question)
    return f


# ════════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

def parse_query(user_question: str, today_str: str) -> dict:
    today = date.fromisoformat(today_str)

    # ── 1. Try Gemini ──────────────────────────────────────────────────────────
    intent = None
    try:
        this_week_start = (today - timedelta(days=today.weekday())).isoformat()
        last_week_start = (today - timedelta(days=today.weekday() + 7)).isoformat()
        last_week_end   = (today - timedelta(days=today.weekday() + 1)).isoformat()
        prompt = (
            _SYSTEM_PROMPT
            .replace("{today}", today_str)
            .replace("{year}", str(today.year))
            .replace("{month}", f"{today.month:02d}")
            .replace("{this_week_start}", this_week_start)
            .replace("{last_week_start}", last_week_start)
            .replace("{last_week_end}",   last_week_end)
            + f"\n\nUser question: {user_question}"
        )
        response = _client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        raw = response.text.strip()
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
        intent = json.loads(raw)
        if "filters" not in intent:
            intent["filters"] = {}
        intent["filters"] = _build_filters(intent["filters"], user_question, today)
        intent = _validate_and_fix(intent, user_question, today)
    except Exception:
        intent = None

    # ── 2. Semantic fallback (Gemini failed or returned generic) ──────────────
    if intent is None or intent.get("query_type") == "generic":
        semantic = _semantic_resolve(user_question, today)
        if semantic:
            # If Gemini gave a richer result (3-level grouping), keep Gemini's
            if (intent is not None
                    and intent.get("tertiary_group_by")
                    and not semantic.get("tertiary_group_by")):
                pass  # keep Gemini intent below
            else:
                return semantic
        # If semantic is None but Gemini gave us something, keep it
        if intent is not None:
            return intent

    if intent is not None:
        return intent

    # ── 3. Keyword fallback ────────────────────────────────────────────────────
    kw = _keyword_fallback(user_question, today)
    if kw:
        return kw

    # ── 4. Safe default ────────────────────────────────────────────────────────
    df, dt, ln = _extract_date_range(user_question, today)
    return {
        "dataset": "creative_performance", "intent": "breakdown",
        "metric": _infer_metric(user_question), "group_by": "Ad name",
        "secondary_group_by": None,
        "sort_by": _infer_metric(user_question), "sort_order": "desc",
        "limit": _extract_limit(user_question),
        "query_type": "generic", "sub_intent": "count",
        "filters": {
            "date_from": df, "date_to": dt, "last_n_days": ln,
            "campaign": None, "pincode": None,
            "creative": _extract_creative_filter(user_question),
            "thresholds": _extract_thresholds(user_question),
        },
    }
