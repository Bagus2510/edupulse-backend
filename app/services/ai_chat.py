import json
import logging
from typing import AsyncGenerator

import asyncpg
from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_google_genai import ChatGoogleGenerativeAI

from app.core.config import settings
from app.services.superset_data import get_dashboard_data

logger = logging.getLogger(__name__)

DEFAULT_CHAT_SYSTEM_PROMPT = """You are a data analyst for an analytics platform.

STRICT RULES:
- Analyze ONLY the real dashboard/chart data provided in the conversation.
- Never invent numbers, trends, chart names, targets, or conclusions.
- FORBIDDEN: generate code, SQL, images, scripts, or anything outside data analysis.
- If asked for something outside data analysis, politely refuse and redirect to the dashboard data.
- Language: {language}

RESPONSE MODE:
1. FACTUAL MODE — Use this for a short question asking for a number, category, comparison,
   or direct value. Answer in 1-3 concise sentences, then include the source chart when known:

**Jawaban:** [direct answer with the exact number]
**Sumber data:** [chart name]

2. ANALYSIS MODE — Use this when the user asks for analysis, summary, insight, pattern,
   trend, recommendation, anomaly, or a complete overview. Use this structure:

## Ringkasan
One short paragraph based on the actual data.

## Temuan Utama
- **[Chart name]:** Specific finding with numbers.
- **[Chart name]:** Specific finding with numbers.

## Tren & Pola
- Mention only patterns supported by the data.
- If there is no time dimension or historical comparison, say that a trend cannot be concluded.

## Rekomendasi
- Recommendations must directly follow from the findings.
- Do not recommend an action when the data is insufficient; state what data is needed instead.

## Potensi Masalah
- Mention only anomalies, gaps, or risks supported by evidence.
- If there is no sufficient evidence, write: "Tidak ditemukan potensi masalah signifikan berdasarkan data yang tersedia."

## Kualitas Data
- State relevant limitations: empty charts, missing fields, sample-only data,
  lack of historical data, or lack of a comparison target.

IMPORTANT FORMATTING RULES:
- Use Markdown headings only in ANALYSIS MODE.
- Use bullet points for lists and bold labels before important values.
- Always include exact numbers, percentages, counts, or averages when available.
- Name the source chart for every important finding whenever known.
- Keep answers concise and do not repeat the same finding in multiple sections.
- Separate facts from interpretation. Do not present an assumption as a fact.

Chart data context will be provided at the start of the conversation. Base every answer on REAL data, not assumptions."""

MAX_HISTORY_MESSAGES = 20

_chart_cache: dict[str, list[dict]] = {}


async def _get_db():
    return await asyncpg.create_pool(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
    )


async def ensure_session(
    session_id: str,
    user_id: int | None = None,
    dashboard_uuid: str = "",
    dashboard_title: str = "",
) -> None:
    """Create session if not exists."""
    pool = await _get_db()
    try:
        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT 1 FROM app.chat_sessions WHERE session_id = $1",
                session_id,
            )
            if not exists:
                title = dashboard_title or "New Chat"
                await conn.execute(
                    """INSERT INTO app.chat_sessions (session_id, user_id, dashboard_uuid, dashboard_title, title)
                       VALUES ($1, $2, $3, $4, $5)""",
                    session_id, user_id, dashboard_uuid, dashboard_title, title,
                )
    finally:
        await pool.close()


async def _save_message(session_id: str, role: str, content: str) -> None:
    """Save a message to DB."""
    pool = await _get_db()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.chat_messages (session_id, role, content) VALUES ($1, $2, $3)",
                session_id, role, content,
            )
            await conn.execute(
                "UPDATE app.chat_sessions SET updated_at = NOW() WHERE session_id = $1",
                session_id,
            )
    finally:
        await pool.close()


async def _load_history(session_id: str) -> InMemoryChatMessageHistory:
    """Load chat history from DB into InMemoryChatMessageHistory."""
    history = InMemoryChatMessageHistory()
    pool = await _get_db()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT role, content FROM app.chat_messages
                   WHERE session_id = $1 ORDER BY created_at ASC""",
                session_id,
            )
            for row in rows:
                if row["role"] == "user":
                    history.add_user_message(row["content"])
                elif row["role"] == "assistant":
                    history.add_ai_message(row["content"])
    finally:
        await pool.close()

    # Trim if too long
    if len(history.messages) > MAX_HISTORY_MESSAGES:
        messages = history.messages
        if len(messages) > 1 and messages[0].content and "Chart data:" in str(messages[0].content):
            context_msg = messages[0]
            recent = messages[-(MAX_HISTORY_MESSAGES - 1):]
            history.messages = [context_msg] + list(recent)
        else:
            history.messages = messages[-MAX_HISTORY_MESSAGES:]

    return history


async def list_sessions(dashboard_uuid: str | None = None) -> list[dict]:
    """List all chat sessions, optionally filtered by dashboard."""
    pool = await _get_db()
    try:
        async with pool.acquire() as conn:
            if dashboard_uuid:
                rows = await conn.fetch(
                    """SELECT session_id, title, dashboard_title, dashboard_uuid,
                              created_at, updated_at
                       FROM app.chat_sessions
                       WHERE dashboard_uuid = $1
                       ORDER BY updated_at DESC""",
                    dashboard_uuid,
                )
            else:
                rows = await conn.fetch(
                    """SELECT session_id, title, dashboard_title, dashboard_uuid,
                              created_at, updated_at
                       FROM app.chat_sessions
                       ORDER BY updated_at DESC"""
                )
            return [dict(r) for r in rows]
    finally:
        await pool.close()


async def get_session_history(session_id: str) -> list[dict]:
    """Get messages for a session."""
    pool = await _get_db()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT role, content, created_at FROM app.chat_messages
                   WHERE session_id = $1 ORDER BY created_at ASC""",
                session_id,
            )
            return [{"role": r["role"], "content": r["content"], "timestamp": str(r["created_at"])} for r in rows]
    finally:
        await pool.close()


async def delete_session(session_id: str) -> None:
    """Delete a chat session and its messages."""
    pool = await _get_db()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM app.chat_sessions WHERE session_id = $1",
                session_id,
            )
    finally:
        await pool.close()


async def clear_chat_history(session_id: str) -> None:
    """Clear chat history for a session."""
    pool = await _get_db()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM app.chat_messages WHERE session_id = $1",
                session_id,
            )
    finally:
        await pool.close()
    logger.info("Cleared chat history for session %s", session_id)


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
    if not dashboard_uuid:
        logger.warning("_get_chart_context called with empty dashboard_uuid")
        return "(Tidak ada UUID dashboard yang diberikan)"

    if dashboard_uuid in _chart_cache:
        charts = _chart_cache[dashboard_uuid]
        logger.info("Using cached chart data for %s (%d charts)", dashboard_uuid, len(charts))
    else:
        try:
            charts = await get_dashboard_data(dashboard_uuid)
            _chart_cache[dashboard_uuid] = charts
            logger.info("Fetched chart data for %s: %d charts", dashboard_uuid, len(charts))
        except Exception as e:
            logger.warning("Failed to fetch chart data for %s: %s", dashboard_uuid, e)
            return "(Gagal mengambil data chart dari Superset)"

    result = _format_chart_data(charts)
    if not result:
        logger.warning("No chart data formatted for %s", dashboard_uuid)
        return "(Tidak ada data chart yang tersedia untuk dashboard ini)"
    return result


def _build_prompt_context(
    dashboard_uuid: str, dashboard_title: str, chart_context: str
) -> str:
    """Build the context message for first message."""
    return (
        f"Dashboard: {dashboard_title}\n"
        f"UUID: {dashboard_uuid}\n\n"
        f"Chart data:\n{chart_context}\n\n"
        f"Sekarang analisis data di atas. User akan bertanya tentang data ini."
    )


def _get_system_prompt() -> str:
    """Get system prompt from config or use default."""
    prompt_template = settings.GEMINI_CHAT_SYSTEM_PROMPT or DEFAULT_CHAT_SYSTEM_PROMPT
    language = settings.AI_ANALYSIS_LANGUAGE
    try:
        return prompt_template.format(language=language)
    except (KeyError, IndexError):
        return prompt_template


async def chat(
    message: str,
    dashboard_uuid: str,
    session_id: str,
    dashboard_title: str = "",
    user_id: int | None = None,
) -> str:
    """Process a chat message with conversational memory."""
    await ensure_session(session_id, user_id=user_id, dashboard_uuid=dashboard_uuid, dashboard_title=dashboard_title)

    llm = _get_llm()
    history = await _load_history(session_id)
    system_prompt = _get_system_prompt()

    chart_context = await _get_chart_context(dashboard_uuid)

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
    ])

    if len(history.messages) == 0:
        context_msg = _build_prompt_context(dashboard_uuid, dashboard_title, chart_context)
        history.add_user_message(context_msg)
        await _save_message(session_id, "user", context_msg)

    history.add_user_message(message)
    await _save_message(session_id, "user", message)

    chain = prompt | llm
    response = await chain.ainvoke({
        "chat_history": history.messages,
        "input": message,
    })

    history.add_ai_message(response.content)
    await _save_message(session_id, "assistant", response.content)

    return response.content


async def chat_stream(
    message: str,
    dashboard_uuid: str,
    session_id: str,
    dashboard_title: str = "",
    user_id: int | None = None,
) -> AsyncGenerator[str, None]:
    """Stream chat response chunk by chunk."""
    await ensure_session(session_id, user_id=user_id, dashboard_uuid=dashboard_uuid, dashboard_title=dashboard_title)

    llm = _get_llm()
    history = await _load_history(session_id)
    system_prompt = _get_system_prompt()

    chart_context = await _get_chart_context(dashboard_uuid)

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
    ])

    if len(history.messages) == 0:
        context_msg = _build_prompt_context(dashboard_uuid, dashboard_title, chart_context)
        history.add_user_message(context_msg)
        await _save_message(session_id, "user", context_msg)

    history.add_user_message(message)
    await _save_message(session_id, "user", message)

    chain = prompt | llm

    full_response = ""
    try:
        async for chunk in chain.astream({
            "chat_history": history.messages,
            "input": message,
        }):
            if chunk.content:
                full_response += chunk.content
                yield chunk.content
    except Exception as e:
        logger.error("Stream error: %s", e)
        error_msg = "Terjadi kesalahan saat memproses response. Silakan coba lagi."
        full_response = error_msg
        yield error_msg

    history.add_ai_message(full_response)
    await _save_message(session_id, "assistant", full_response)
