"""
query_router.py — Routes natural language questions to the correct precomputed view.

Logic:
1. Identify the intended view (Creative, Pincode, Consumption, Winning).
2. Map to the specific Google Sheet tab.
3. Extract basic entities for filtering downstream.
"""

import logging

logger = logging.getLogger(__name__)

def route_query(question: str) -> dict:
    """
    Step 1: QUERY UNDERSTANDING
    Step 2: DATASET SELECTION
    Map query to correct precomputed sheet.
    """
    q_lower = question.lower()
    
    # Mapping queries to sheets (as per Step 2 rules)
    sheet_map = {
        "Daily_PC_Consumption": ["pincode day", "pincode usage", "daily pincode", "usage count"],
        "PC_Creative_Date_View": ["pincode wise", "pc wise", "pincode performance", "pincode split"],
        "Creative_Performance_View": ["creative performance", "ad performance", "image performance"],
        "Winning_Creatives_View": ["winning creatives", "best creatives", "top creatives"]
    }
    
    selected_sheet = "Raw Dump" # Fallback
    
    # Detect Sheet
    for sheet, keywords in sheet_map.items():
        if any(k in q_lower for k in keywords):
            selected_sheet = sheet
            break

    # Basic Intent Detection
    intent = "general_analysis"
    if "trend" in q_lower or "over time" in q_lower:
        intent = "trend_analysis"
    elif "winning" in q_lower or "best" in q_lower:
        intent = "performance_check"

    # Detect Entity
    entity = "creative"
    if "pincode" in q_lower or "pc" in q_lower:
        entity = "pincode"
    elif "campaign" in q_lower:
        entity = "campaign"
        
    logger.info(f"Routed '{question}' → Sheet: {selected_sheet} | Intent: {intent}")
    
    return {
        "intent": intent,
        "selected_sheet": selected_sheet,
        "entity": entity,
        "params": {} # Extraction of specific filters happens in query_layer
    }
