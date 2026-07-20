import json

from google import genai
from google.genai import types

from app.core.config import settings

SYSTEM_PROMPT = """You are an academic data analyst. Given a dashboard title and description,
generate a structured insight in JSON format with these fields:
- dashboard_type: string (category of the dashboard)
- summary: string (2-3 sentence overview)
- key_findings: list of 3-4 strings (notable observations)
- trend: string (one of: "naik", "turun", "stabil")
- business_recommendation: string (actionable recommendation)
- potential_issue: string or null (concern to watch)
- confidence: float (0.0-1.0, how confident in the analysis)

Respond ONLY with valid JSON. No markdown, no extra text."""


def _get_client() -> genai.Client:
    return genai.Client(api_key=settings.GEMINI_API_KEY)


async def analyze_dashboard(title: str, description: str) -> dict:
    client = _get_client()

    response = await client.aio.models.generate_content(
        model=settings.GEMINI_MODEL,
        contents=f"Dashboard: {title}\nDescription: {description}\n\nGenerate insight:",
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
        ),
    )

    text = response.text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    return json.loads(text)
