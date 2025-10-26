"""
Search-backed value assessment that surfaces independent USD and RMB market prices.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Dict, Optional, Tuple

from query_agent.search import SearchError, SearchResult, serper_search


DEFAULT_TIME_HOURS = {
    "L3": 16.0,   # approx two workdays at 8h/day
    "L4": 56.0,   # approx seven workdays
    "L5": 240.0,  # approx six workweeks
}

# Baseline hourly rates for the US market (USD).
DEFAULT_RATE_USD = {
    "L3": 80.0,
    "L4": 120.0,
    "L5": 180.0,
}

# Baseline hourly rates for the China market (CNY). Chosen to reflect typical metro-market consulting rates.
DEFAULT_RATE_CNY = {
    "L3": 600.0,
    "L4": 900.0,
    "L5": 3000.0,
}

RATE_SEARCH_QUERY_USD = {
    "L3": "average hourly rate for ai product manager or ml engineer 2024 united states",
    "L4": "senior ai consultant hourly rate 2024 united states",
    "L5": "enterprise ai strategy consultant hourly rate united states 2024",
}

RATE_SEARCH_QUERY_CNY = {
    "L3": "中国 AI 产品经理 自由职业 小时 费率 2024 人民币",
    "L4": "中国 AI 顾问 每小时 价格 2024 人民币",
    "L5": "中国 AI 战略 顾问 小时 收费 2024 人民币",
}


def _env_override(level: str, suffix: str, fallback: float) -> float:
    key = f"VALUE_{level}_{suffix}"
    value = os.environ.get(key)
    if value is None:
        return fallback
    try:
        return float(value)
    except ValueError:
        return fallback


def _extract_amount(text: str, currency: str) -> Optional[float]:
    """
    Parse a numeric amount from text based on the currency marker.
    """
    if not text:
        return None

    sanitized = text.replace(",", "")
    sanitized = re.sub(r"[*_]", "", sanitized)
    currency = currency.upper()

    if currency == "USD":
        priority_patterns = [
            r"\$ ?(\d+(?:\.\d+)?)\s*/\s*(?:hour|hr)",
            r"hourly\s*(?:rate|pay)\s*(?:of)?\s*\$ ?(\d+(?:\.\d+)?)",
            r"\$ ?(\d+(?:\.\d+)?)\s*(?:per|an)\s*(?:hour|hr)",
        ]
    else:
        priority_patterns = [
            r"¥ ?(\d+(?:\.\d+)?)\s*/\s*(?:小时|hour|hr)",
            r"每\s*(?:小时|hour|hr)\s*¥? ?(\d+(?:\.\d+)?)",
            r"小时费率\s*¥? ?(\d+(?:\.\d+)?)",
            r"¥ ?(\d+(?:\.\d+)?)\s*(?:每|/)\s*(?:小时|hour|hr)",
        ]

    for pattern in priority_patterns:
        match = re.search(pattern, sanitized, flags=re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except (IndexError, ValueError):
                continue

    patterns_usd = [
        r"\$ ?(\d+(?:\.\d+)?)",
        r"USD\s*(\d+(?:\.\d+)?)",
    ]
    patterns_cny = [
        r"¥ ?(\d+(?:\.\d+)?)",
        r"RMB\s*(\d+(?:\.\d+)?)",
        r"人民币\s*(\d+(?:\.\d+)?)\s*元?",
        r"(\d+(?:\.\d+)?)\s*元/小时",
    ]

    patterns = patterns_usd if currency == "USD" else patterns_cny
    for pattern in patterns:
        matches = re.findall(pattern, sanitized, flags=re.IGNORECASE)
        if not matches:
            continue
        values = [float(m) for m in matches if m]
        if not values:
            continue
        if len(values) == 1:
            return values[0]
        # Multiple matches typically indicates a range. Average the first two entries.
        return sum(values[:2]) / min(len(values[:2]), 2)
    return None


def _best_result_with_rate(results: Tuple[SearchResult, ...], currency: str) -> Tuple[Optional[float], Optional[Dict[str, Optional[str]]]]:
    for result in results:
        amount = _extract_amount(result.snippet, currency) or _extract_amount(result.title, currency)
        if amount is None:
            continue
        return amount, result.to_dict()
    return None, None


def _rate_defaults(level: str, currency: str) -> float:
    level = level.upper()
    if currency.upper() == "USD":
        return DEFAULT_RATE_USD.get(level, 0.0)
    return DEFAULT_RATE_CNY.get(level, 0.0)


def _rate_query(level: str, currency: str) -> str:
    level = level.upper()
    if currency.upper() == "USD":
        return RATE_SEARCH_QUERY_USD.get(level, "ai consultant hourly rate united states 2024")
    return RATE_SEARCH_QUERY_CNY.get(level, "中国 ai 顾问 小时 费率 2024 人民币")


@lru_cache(maxsize=6)
def _lookup_hourly_rate(level: str, currency: str) -> Tuple[float, Optional[Dict[str, Optional[str]]], str]:
    """
    Fetch an hourly rate from web search for the specified currency market.
    """
    currency = currency.upper()
    default_rate = _rate_defaults(level, currency)
    query = _rate_query(level, currency)

    try:
        results = tuple(serper_search(query, num=5))
    except SearchError as exc:
        unit = "$" if currency == "USD" else "¥"
        explanation = f"{currency} 市场搜索失败（{exc}），使用默认费率 {unit}{default_rate:.2f}/小时。"
        return default_rate, None, explanation

    rate, reference = _best_result_with_rate(results, currency)
    if rate is None:
        unit = "$" if currency == "USD" else "¥"
        explanation = f"{currency} 市场搜索未解析到金额，使用默认费率 {unit}{default_rate:.2f}/小时。"
        return default_rate, None, explanation

    unit = "$" if currency == "USD" else "¥"
    explanation = (
        f"{currency} 市场费率参考搜索“{query}”，使用首条可解析金额的结果作为参考值（约 {unit}{rate:.2f}/小时）。"
    )
    return rate, reference, explanation


def estimate_value(level: str) -> Dict[str, object]:
    """
    Estimate human time (hours) and independent USD/CNY market values for a given level.
    """
    level = level.upper()
    hours = _env_override(level, "HOURS", DEFAULT_TIME_HOURS.get(level, 0.0))

    rate_usd, usd_reference, usd_explanation = _lookup_hourly_rate(level, "USD")
    rate_usd = _env_override(level, "RATE_USD", rate_usd)
    rate_usd = _env_override(level, "RATE", rate_usd)  # backwards compatibility

    rate_cny, cny_reference, cny_explanation = _lookup_hourly_rate(level, "CNY")
    rate_cny = _env_override(level, "RATE_CNY", rate_cny)

    usd_value = hours * rate_usd
    cny_value = hours * rate_cny

    explanation_usd = (
        f"{usd_explanation} 估算成本 = {hours:.0f} 小时 × ${rate_usd:.2f}/小时 ≈ ${usd_value:,.2f}。"
    )
    explanation_cny = (
        f"{cny_explanation} 估算成本 = {hours:.0f} 小时 × ¥{rate_cny:.2f}/小时 ≈ ¥{cny_value:,.2f}。"
    )

    return {
        "hours": hours,
        "hourly_rate": rate_usd,  # retained for compatibility
        "hourly_rate_usd": rate_usd,
        "hourly_rate_usd_reference": usd_reference,
        "hourly_rate_cny": rate_cny,
        "hourly_rate_cny_reference": cny_reference,
        "estimated_value": usd_value,
        "estimated_value_usd": usd_value,
        "estimated_value_cny": cny_value,
        "value_explanation": f"{explanation_usd} {explanation_cny}",
        "value_explanation_usd": explanation_usd,
        "value_explanation_cny": explanation_cny,
    }


def scenario_value_aggregate(levels: Dict[str, Dict[str, object]]) -> Dict[str, float]:
    """
    Aggregate values across levels for a scenario.
    """
    total_hours = sum(float(info.get("hours", 0.0)) for info in levels.values())
    total_value_usd = sum(float(info.get("estimated_value_usd", info.get("estimated_value", 0.0))) for info in levels.values())
    total_value_cny = sum(float(info.get("estimated_value_cny", 0.0)) for info in levels.values())
    return {
        "total_hours": total_hours,
        "total_value": total_value_usd,
        "total_value_usd": total_value_usd,
        "total_value_cny": total_value_cny,
    }
