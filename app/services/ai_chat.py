import json
import logging
from collections import defaultdict

from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_google_genai import ChatGoogleGenerativeAI

from app.core.config import settings
from app.services.superset_data import get_dashboard_data

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Kamu adalah analis data akademik untuk platform EduPulse Analytics.

ATURAN KETAT:
- Tugas: menganalisis data dashboard pendidikan berdasarkan chart data yang diberikan
- Output: HANYA analisis (ringkasan, temuan, tren, rekomendasi, potensi masalah)
- DILARANG: generate code, SQL, gambar, script, atau apapun selain analisis data
- Jika diminta hal di luar analisis data, tolak dengan sopan dan arahkan ke topik analisis
- Bahasa: Indonesia
- Format: gunakan markdown untuk struktur (bold, list, heading)

Konteks chart data akan diberikan di awal percakapan. Analisis berdasarkan data NYATA, bukan asumsi."""

# In-memory session store: session_id -> InMemoryChatMessageHistory
_sessions: dict[str, InMemoryChatMessageHistory] = defaultdict(InMemoryChatMessageHistory)

# Cache chart data per dashboard to avoid repeated Superset calls
_chart_cache: dict[str, list[dict]] = {}


def _get_llm() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model=settings.GEMINI_MODEL,
        google_api_key=settings.GEMINI_API_KEY,
        temperature=0.3,
        max_output_tokens=4096,
    )


def _format_chart_data(charts: list[dict]) -> str:
    """Format chart data for context."""
    parts = []
    for chart in charts:
        name = chart.get("name", "Unknown")
        viz = chart.get("viz_type", "unknown")
        data = chart.get("data")

        if not data:
            parts.append(f"Chart: {name} (type: {viz}) — no data available")
            continue

        rows_count = len(data)
        sample = data[:10]

        parts.append(f"Chart: {name} (type: {viz}, {rows_count} rows)")
        parts.append(f"Data sample: {json.dumps(sample, default=str, ensure_ascii=False)}")

        if data and isinstance(data[0], dict):
            for col in data[0].keys():
                values = [r.get(col) for r in data if r.get(col) is not None]
                numeric = [v for v in values if isinstance(v, (int, float))]
                if numeric and len(numeric) > 1:
                    avg = sum(numeric) / len(numeric)
                    parts.append(f"  {col}: avg={avg:.2f}, min={min(numeric)}, max={max(numeric)}")

    return "\n".join(parts)


async def _get_chart_context(dashboard_uuid: str) -> str:
    """Get formatted chart data for a dashboard."""
    if dashboard_uuid in _chart_cache:
        charts = _chart_cache[dashboard_uuid]
    else:
        try:
            charts = await get_dashboard_data(dashboard_uuid)
            _chart_cache[dashboard_uuid] = charts
        except Exception as e:
            logger.warning("Failed to fetch chart data: %s", e)
            return "(Gagal mengambil data chart dari Superset)"

    return _format_chart_data(charts)


def get_chat_history(session_id: str) -> InMemoryChatMessageHistory:
    """Get chat history for a session."""
    return _sessions[session_id]


def clear_chat_history(session_id: str) -> None:
    """Clear chat history for a session."""
    if session_id in _sessions:
        del _sessions[session_id]
    # Also clear chart cache if needed
    logger.info("Cleared chat history for session %s", session_id)


async def chat(
    message: str,
    dashboard_uuid: str,
    session_id: str,
    dashboard_title: str = "",
) -> str:
    """Process a chat message with conversational memory."""
    llm = _get_llm()
    history = get_chat_history(session_id)

    # Get chart context
    chart_context = await _get_chart_context(dashboard_uuid)

    # Build prompt
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
    ])

    # If this is first message, add chart context
    if len(history.messages) == 0:
        context_msg = (
            f"Dashboard: {dashboard_title}\n"
            f"UUID: {dashboard_uuid}\n\n"
            f"Chart data:\n{chart_context}\n\n"
            f"Sekarang analisis data di atas. User akan bertanya tentang data ini."
        )
        history.add_user_message(context_msg)

    # Add user message
    history.add_user_message(message)

    # Invoke chain
    chain = prompt
    response = await chain.ainvoke({
        "chat_history": history.messages,
        "input": message,
    })

    # Add AI response to history
    history.add_ai_message(response.content)

    return response.content
