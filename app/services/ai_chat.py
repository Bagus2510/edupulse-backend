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
- Task: analyze dashboard data based on the chart data provided
- Output: ONLY analysis (summary, findings, trends, recommendations, potential issues)
- FORBIDDEN: generate code, SQL, images, scripts, or anything other than data analysis
- If asked for something outside data analysis, politely refuse and redirect to analysis topics
- Language: {language}
- Format: ALWAYS use this exact markdown structure for EVERY analysis response:

## Ringkasan
One paragraph summary of the overall data.

## Temuan Utama
- **Finding 1:** Description with specific numbers
- **Finding 2:** Description with specific numbers
- (add more as needed)

## Tren & Pola
- Bullet points for each trend observed
- Use specific numbers and percentages from the data

## Rekomendasi
- **Recommendation 1:** Actionable suggestion
- **Recommendation 2:** Actionable suggestion
- (add more as needed)

## Potensi Masalah
- List any concerning patterns or anomalies found
- If none, state "Tidak ditemukan potensi masalah signifikan"

IMPORTANT FORMATTING RULES:
- ALWAYS start with ## headings
- ALWAYS use bold **label:** before each point
- ALWAYS use bullet points (-) for lists
- ALWAYS include specific numbers from the data (percentages, counts, averages)
- NEVER write long paragraphs without line breaks
- NEVER put all text in one block — always separate into sections

Chart data context will be provided at the start of the conversation. Base analysis on REAL data, not assumptions."""

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
