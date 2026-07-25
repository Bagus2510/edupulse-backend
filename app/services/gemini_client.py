import json

from google import genai
from google.genai import types

from app.core.config import settings

SYSTEM_PROMPT = """You are an academic data analyst for EduPulse, an education analytics platform.

Given dashboard title and REAL chart data from Apache Superset, generate a structured insight.

Rules:
- Base analysis on ACTUAL data values, not assumptions
- Identify real patterns, outliers, and trends from the numbers
- Provide specific numbers in findings (e.g., "rata-rata nilai 78.5")
- If data is empty or limited, acknowledge it honestly

Respond ONLY with valid JSON with these fields:
{
  "dashboard_type": "string (category: akademik, keuangan, operasional, etc)",
  "summary": "string (2-3 sentences overview based on real data)",
  "key_findings": ["string (specific finding with numbers)", ...],
  "trend": "string (one of: naik, turun, stabil)",
  "business_recommendation": "string (actionable recommendation based on data)",
  "potential_issue": "string or null (concern to watch)",
  "confidence": "float (0.0-1.0, based on data quality/quantity)"
}

No markdown, no extra text. Only JSON."""


def _get_client() -> genai.Client:
    return genai.Client(api_key=settings.GEMINI_API_KEY)


def _format_chart_data(charts: list[dict]) -> str:
    """Format chart data into readable string for Gemini."""
    parts = []
    for chart in charts:
        name = chart.get("name", "Unknown")
        viz = chart.get("viz_type", "unknown")
        data = chart.get("data")

        if not data:
            parts.append(f"Chart: {name} (type: {viz}) — no data available")
            continue

        rows_count = len(data)
        sample = data[:10]  # first 10 rows

        parts.append(f"Chart: {name} (type: {viz}, {rows_count} rows)")
        parts.append(f"Sample data: {json.dumps(sample, default=str, ensure_ascii=False)}")

        # Compute basic stats if numeric columns exist
        if data and isinstance(data[0], dict):
            for col in data[0].keys():
                values = [r.get(col) for r in data if r.get(col) is not None]
                numeric = [v for v in values if isinstance(v, (int, float))]
                if numeric and len(numeric) > 1:
                    avg = sum(numeric) / len(numeric)
                    parts.append(f"  {col}: avg={avg:.2f}, min={min(numeric)}, max={max(numeric)}")

    return "\n".join(parts)


async def analyze_dashboard(
    title: str,
    description: str,
    chart_data: list[dict] | None = None,
) -> dict:
    client = _get_client()

    if chart_data:
        data_str = _format_chart_data(chart_data)
        user_msg = (
            f"Dashboard: {title}\n"
            f"Description: {description}\n\n"
            f"Real chart data from Superset:\n{data_str}\n\n"
            f"Generate insight based on this data:"
        )
    else:
        user_msg = (
            f"Dashboard: {title}\n"
            f"Description: {description}\n"
            f"No chart data available. Generate insight based on title and description only:"
        )

    response = await client.aio.models.generate_content(
        model=settings.GEMINI_MODEL,
        contents=user_msg,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
        ),
    )

    text = response.text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    return json.loads(text)
