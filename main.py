import asyncio
import base64
import html
import json
import re
import threading
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
import fastapi
import pgvector
import uvicorn
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.event.filter import PermissionType
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api.util import SessionController, session_waiter


PLUGIN_NAME = "astrbot_plugin_pg_memory"
DEFAULT_ADMIN = "0"


def now_ts() -> int:
    return int(time.time())


def fmt_time(ts: int | None) -> str:
    if not ts:
        return "N/A"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(int(ts)))


def clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def split_text(text: str, max_len: int = 1200) -> list[str]:
    chunks: list[str] = []
    curr = str(text or "").strip()
    while len(curr) > max_len:
        idx = curr.rfind("\n", 0, max_len)
        if idx < 200:
            idx = max_len
        chunks.append(curr[:idx].strip())
        curr = curr[idx:].strip()
    if curr:
        chunks.append(curr)
    return chunks or [text]


def platform_id(event: AstrMessageEvent) -> str:
    for getter in ("get_platform_id",):
        try:
            val = getattr(event, getter)()
            if val:
                return str(val)
        except Exception:
            pass
    umo = str(getattr(event, "unified_msg_origin", "") or "")
    if ":" in umo:
        return umo.split(":", 1)[0] or "onebot"
    return "onebot"


def group_id_of(event: AstrMessageEvent) -> str:
    try:
        val = event.get_group_id()
        if val:
            return str(val)
    except Exception:
        pass
    return str(getattr(event.message_obj, "group_id", "") or "").strip()


def sender_id_of(event: AstrMessageEvent) -> str:
    try:
        val = event.get_sender_id()
        if val:
            return str(val)
    except Exception:
        pass
    sender = getattr(getattr(event, "message_obj", None), "sender", None)
    return str(getattr(sender, "user_id", "") or "").strip()


def session_id_of(event: AstrMessageEvent) -> str:
    sid = str(getattr(event, "unified_msg_origin", "") or "").strip()
    if sid:
        return sid
    gid = group_id_of(event)
    if gid:
        return f"{platform_id(event)}:GroupMessage:{gid}"
    uid = sender_id_of(event)
    return f"{platform_id(event)}:FriendMessage:{uid}"


def raw_message_text(event: AstrMessageEvent) -> str:
    for attr in ("message_str", "text"):
        val = str(getattr(event, attr, "") or "").strip()
        if val:
            return val
    return ""


def group_name_of(event: AstrMessageEvent) -> str:
    obj = getattr(event, "message_obj", None)
    for attr in ("group_name",):
        val = str(getattr(obj, attr, "") or "").strip()
        if val:
            return val
    group = getattr(obj, "group", None)
    val = str(getattr(group, "group_name", "") or "").strip()
    if val:
        return val
    return group_id_of(event)


def event_raw_message_dict(event: AstrMessageEvent) -> dict[str, Any]:
    obj = getattr(event, "message_obj", None)
    raw = getattr(obj, "raw_message", None)
    if isinstance(raw, dict):
        data = dict(raw)
    else:
        data = {}
    gid = group_id_of(event)
    sender = getattr(obj, "sender", None)
    if gid:
        data.setdefault("group_id", gid)
    data.setdefault("message_id", str(getattr(obj, "message_id", "") or ""))
    data.setdefault("time", int(getattr(obj, "timestamp", 0) or now_ts()))
    data.setdefault("user_id", sender_id_of(event))
    if "sender" not in data:
        data["sender"] = {
            "user_id": sender_id_of(event),
            "nickname": str(getattr(sender, "nickname", "") or sender_id_of(event)),
        }
    if "message" not in data:
        data["message"] = [{"type": "text", "data": {"text": raw_message_text(event)}}]
    return data


def unique_message_key(raw_msg: dict[str, Any]) -> str:
    for key in ("message_id", "message_seq", "real_id", "seq"):
        val = raw_msg.get(key)
        if val is not None and val != "":
            return f"{key}:{val}"
    return (
        f"time:{raw_msg.get('time', '')}:"
        f"user:{raw_msg.get('user_id', '')}:"
        f"msg:{raw_msg.get('message', '')}"
    )


def extract_plain_content(raw_msg: dict[str, Any]) -> tuple[str, dict[str, int]]:
    message = raw_msg.get("message", "")
    type_counts: dict[str, int] = {}
    parts: list[str] = []

    def add_type(seg_type: str) -> None:
        type_counts[seg_type] = type_counts.get(seg_type, 0) + 1

    if isinstance(message, list):
        for seg in message:
            if not isinstance(seg, dict):
                text = str(seg).strip()
                if text:
                    parts.append(text)
                continue
            seg_type = str(seg.get("type") or "unknown")
            add_type(seg_type)
            data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
            if seg_type == "text":
                text = str(data.get("text") or "")
                if text:
                    parts.append(text)
            elif seg_type == "at":
                qq = str(data.get("qq") or "").strip()
                parts.append(f"@{qq}" if qq else "@")
            elif seg_type == "face":
                parts.append("[表情]")
            elif seg_type == "image":
                parts.append("[图片]")
            elif seg_type == "video":
                parts.append("[视频]")
            elif seg_type == "record":
                parts.append("[语音]")
            elif seg_type == "file":
                file_name = str(data.get("name") or data.get("file") or "").strip()
                parts.append(f"[文件:{file_name}]" if file_name else "[文件]")
            elif seg_type == "reply":
                parts.append("[回复]")
            elif seg_type == "forward":
                parts.append("[转发消息]")
            elif seg_type in {"json", "xml"}:
                parts.append("[卡片消息]")
            else:
                parts.append(f"[{seg_type}]")
    else:
        text = str(message or "").strip()
        if text:
            parts.append(text)

    content = " ".join(part.strip() for part in parts if part and part.strip())
    return content or "[空消息]", type_counts


def build_message_row(
    platform: str, group_id: str, group_name: str, raw_msg: dict[str, Any]
) -> dict[str, Any] | None:
    content, segment_counts = extract_plain_content(raw_msg)
    sender = raw_msg.get("sender") if isinstance(raw_msg.get("sender"), dict) else {}
    user_id = str(raw_msg.get("user_id") or sender.get("user_id") or "")
    user_name = str(
        sender.get("card")
        or sender.get("nickname")
        or raw_msg.get("sender_name")
        or user_id
        or "unknown"
    )
    message_id = str(
        raw_msg.get("message_id")
        or raw_msg.get("message_seq")
        or raw_msg.get("real_id")
        or unique_message_key(raw_msg)
    )
    try:
        msg_time = int(raw_msg.get("time") or 0)
    except (TypeError, ValueError):
        msg_time = 0
    if msg_time <= 0:
        return None
    try:
        real_seq = int(raw_msg.get("real_seq") or 0)
    except (TypeError, ValueError):
        real_seq = 0
    return {
        "platform": platform,
        "group_id": str(group_id),
        "group_name": str(group_name or group_id),
        "message_id": message_id,
        "message_seq": str(raw_msg.get("message_seq") or ""),
        "real_seq": real_seq,
        "time": msg_time,
        "user_id": user_id,
        "user_name": user_name,
        "content": content,
        "segment_counts": json.dumps(segment_counts, ensure_ascii=False),
    }


def anchor_candidates(raw_msg: dict[str, Any]) -> list[Any]:
    candidates: list[Any] = []
    try:
        real_seq = int(raw_msg.get("real_seq") or 0)
    except (TypeError, ValueError):
        real_seq = 0
    for key in ("message_seq", "real_id", "seq", "message_id"):
        val = raw_msg.get(key)
        if val is None or val == "":
            continue
        candidates.append(val)
        if key == "message_seq" and real_seq > 0:
            candidates.append(f"packet:{real_seq}")
    out: list[Any] = []
    seen: set[str] = set()
    for val in candidates:
        text = str(val)
        if text not in seen:
            seen.add(text)
            out.append(val)
    return out


@dataclass
class ArchiveStats:
    group_id: str
    group_name: str
    pages: int = 0
    fetched: int = 0
    inserted: int = 0
    duplicates: int = 0
    oldest_time: int = 0
    newest_time: int = 0
    exhausted: bool = False
    error: str = ""


class PgStore:
    def __init__(self, plugin: "PgMemoryPlugin"):
        self.plugin = plugin
        self.pool: Any = None
        self.embedding_dim = plugin.embedding_dim
        self._lock = asyncio.Lock()
        self._last_error = ""

    @property
    def ready(self) -> bool:
        return self.pool is not None

    @property
    def last_error(self) -> str:
        return self._last_error

    async def ensure(self) -> bool:
        if self.pool:
            return True
        async with self._lock:
            if self.pool:
                return True
            try:
                import asyncpg

                db_cfg = self.plugin.cfg("database", {})
                self.pool = await asyncpg.create_pool(
                    host=str(db_cfg.get("host") or "postgres"),
                    port=int(db_cfg.get("port") or 5432),
                    database=str(db_cfg.get("database") or "astrbot_memory"),
                    user=str(db_cfg.get("user") or "astrbot_memory"),
                    password=str(db_cfg.get("password") or "astrbot_memory"),
                    min_size=clamp_int(db_cfg.get("min_size"), 1, 1, 20),
                    max_size=clamp_int(db_cfg.get("max_size"), 5, 1, 50),
                )
                await self.init_schema()
                self._last_error = ""
                return True
            except Exception as exc:
                self.pool = None
                self._last_error = str(exc)
                logger.error(f"[pg_memory] PostgreSQL 初始化失败: {exc}", exc_info=True)
                return False

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def init_schema(self) -> None:
        dim = int(self.embedding_dim or 3072)
        async with self.pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            try:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            except Exception as exc:
                logger.warning(f"[pg_memory] pg_trgm 不可用，跳过文本 trigram 索引: {exc}")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS memories (
                    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id text NOT NULL DEFAULT '',
                    platform text NOT NULL DEFAULT '',
                    group_id text NOT NULL DEFAULT '',
                    session_id text NOT NULL DEFAULT '',
                    personality_id text NOT NULL DEFAULT '',
                    scope text NOT NULL DEFAULT 'session',
                    memory_type text NOT NULL DEFAULT 'summary',
                    content text NOT NULL,
                    summary text NOT NULL DEFAULT '',
                    importance real NOT NULL DEFAULT 0.5,
                    tags jsonb NOT NULL DEFAULT '[]'::jsonb,
                    metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                    embedding vector({dim}),
                    source text NOT NULL DEFAULT '',
                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    last_used_at timestamptz,
                    deleted_at timestamptz
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS archived_messages (
                    id bigserial PRIMARY KEY,
                    platform text NOT NULL,
                    group_id text NOT NULL,
                    group_name text NOT NULL DEFAULT '',
                    message_id text NOT NULL,
                    message_seq text NOT NULL DEFAULT '',
                    real_seq bigint NOT NULL DEFAULT 0,
                    time bigint NOT NULL,
                    user_id text NOT NULL DEFAULT '',
                    user_name text NOT NULL DEFAULT '',
                    content text NOT NULL,
                    segment_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    UNIQUE(platform, group_id, message_id)
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS archive_state (
                    platform text NOT NULL,
                    group_id text NOT NULL,
                    group_name text NOT NULL DEFAULT '',
                    total_messages bigint NOT NULL DEFAULT 0,
                    oldest_time bigint,
                    newest_time bigint,
                    last_anchor text NOT NULL DEFAULT '',
                    last_real_seq bigint NOT NULL DEFAULT 0,
                    exhausted boolean NOT NULL DEFAULT false,
                    last_error text NOT NULL DEFAULT '',
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY(platform, group_id)
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS message_counters (
                    session_id text PRIMARY KEY,
                    counter int NOT NULL DEFAULT 0,
                    last_summary_at timestamptz,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_results (
                    id bigserial PRIMARY KEY,
                    platform text NOT NULL DEFAULT '',
                    group_id text NOT NULL,
                    group_name text NOT NULL DEFAULT '',
                    analysis_type text NOT NULL DEFAULT 'manual',
                    result_data jsonb NOT NULL,
                    message_count int NOT NULL DEFAULT 0,
                    time_range_start bigint,
                    time_range_end bigint,
                    created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_archived_group_time ON archived_messages(platform, group_id, time)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_archived_user_time ON archived_messages(user_id, time)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memories_session ON memories(session_id, created_at DESC) WHERE deleted_at IS NULL"
            )
            try:
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_memories_embedding ON memories USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL AND deleted_at IS NULL"
                )
            except Exception as exc:
                logger.warning(f"[pg_memory] HNSW 索引暂不可用，继续运行: {exc}")

    async def insert_messages(
        self, rows: list[dict[str, Any]]
    ) -> tuple[int, int]:
        if not rows or not await self.ensure():
            return 0, len(rows)
        inserted = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for row in rows:
                    status = await conn.execute(
                        """
                        INSERT INTO archived_messages (
                            platform, group_id, group_name, message_id, message_seq,
                            real_seq, time, user_id, user_name, content, segment_counts
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
                        ON CONFLICT(platform, group_id, message_id) DO NOTHING
                        """,
                        row["platform"],
                        row["group_id"],
                        row["group_name"],
                        row["message_id"],
                        row["message_seq"],
                        row["real_seq"],
                        row["time"],
                        row["user_id"],
                        row["user_name"],
                        row["content"],
                        row["segment_counts"],
                    )
                    if status.endswith("1"):
                        inserted += 1
        return inserted, len(rows) - inserted

    async def update_archive_state(
        self, platform: str, stats: ArchiveStats, last_anchor: str = "", last_real_seq: int = 0
    ) -> None:
        if not await self.ensure():
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO archive_state (
                    platform, group_id, group_name, total_messages, oldest_time,
                    newest_time, last_anchor, last_real_seq, exhausted, last_error
                )
                VALUES (
                    $1, $2, $3,
                    (SELECT COUNT(*) FROM archived_messages WHERE platform=$1 AND group_id=$2),
                    $4, $5, $6, $7, $8, $9
                )
                ON CONFLICT(platform, group_id) DO UPDATE SET
                    group_name=excluded.group_name,
                    total_messages=excluded.total_messages,
                    oldest_time=excluded.oldest_time,
                    newest_time=excluded.newest_time,
                    last_anchor=excluded.last_anchor,
                    last_real_seq=excluded.last_real_seq,
                    exhausted=excluded.exhausted,
                    last_error=excluded.last_error,
                    updated_at=now()
                """,
                platform,
                stats.group_id,
                stats.group_name,
                stats.oldest_time or None,
                stats.newest_time or None,
                last_anchor,
                int(last_real_seq or 0),
                bool(stats.exhausted),
                stats.error or "",
            )

    async def refresh_archive_state(self, platform: str, group_id: str, group_name: str) -> None:
        if not await self.ensure():
            return
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS total, MIN(time) AS oldest_time, MAX(time) AS newest_time,
                       MAX(real_seq) AS last_real_seq
                FROM archived_messages
                WHERE platform = $1 AND group_id = $2
                """,
                platform,
                str(group_id),
            )
            await conn.execute(
                """
                INSERT INTO archive_state (
                    platform, group_id, group_name, total_messages, oldest_time,
                    newest_time, last_anchor, last_real_seq, exhausted, last_error
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false,'')
                ON CONFLICT(platform, group_id) DO UPDATE SET
                    group_name = excluded.group_name,
                    total_messages = excluded.total_messages,
                    oldest_time = excluded.oldest_time,
                    newest_time = excluded.newest_time,
                    last_real_seq = GREATEST(archive_state.last_real_seq, excluded.last_real_seq),
                    updated_at = now()
                """,
                platform,
                str(group_id),
                str(group_name or group_id),
                int(row["total"] or 0),
                row["oldest_time"],
                row["newest_time"],
                "",
                int(row["last_real_seq"] or 0),
            )

    async def overview(self) -> dict[str, Any]:
        if not await self.ensure():
            return {"error": self.last_error}
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS total, COUNT(DISTINCT group_id) AS groups,
                       MIN(time) AS oldest, MAX(time) AS newest
                FROM archived_messages
                """
            )
            states = await conn.fetch(
                """
                SELECT group_id, group_name, total_messages, oldest_time, newest_time,
                       exhausted, last_error, updated_at
                FROM archive_state
                ORDER BY total_messages DESC, group_id
                LIMIT 20
                """
            )
        return {
            "total": int(row["total"] or 0),
            "groups": int(row["groups"] or 0),
            "oldest": int(row["oldest"] or 0),
            "newest": int(row["newest"] or 0),
            "states": states,
        }

    async def fetch_messages(
        self, group_id: str | None = None, days: int | None = None, limit: int | None = None
    ) -> list[Any]:
        if not await self.ensure():
            return []
        where: list[str] = []
        params: list[Any] = []
        if group_id:
            params.append(str(group_id))
            where.append(f"group_id = ${len(params)}")
        if days:
            params.append(now_ts() - int(days) * 86400)
            where.append(f"time >= ${len(params)}")
        where_sql = "WHERE " + " AND ".join(where) if where else ""
        limit_sql = f"LIMIT {int(limit)}" if limit else ""
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                f"""
                SELECT group_id, group_name, time, user_id, user_name, content,
                       message_id, message_seq, real_seq
                FROM archived_messages
                {where_sql}
                ORDER BY group_id, time
                {limit_sql}
                """,
                *params,
            )

    async def save_memory(
        self,
        *,
        user_id: str,
        platform: str,
        group_id: str,
        session_id: str,
        content: str,
        memory_type: str,
        source: str,
        embedding: list[float] | None,
        metadata: dict[str, Any] | None = None,
        importance: float = 0.5,
    ) -> str:
        if not await self.ensure():
            raise RuntimeError(self.last_error or "PostgreSQL is unavailable")
        vector = "[" + ",".join(str(float(x)) for x in embedding) + "]" if embedding else None
        async with self.pool.acquire() as conn:
            if vector:
                row = await conn.fetchrow(
                    """
                    INSERT INTO memories (
                        user_id, platform, group_id, session_id, content, memory_type,
                        source, embedding, metadata, importance
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::vector,$9::jsonb,$10)
                    RETURNING id
                    """,
                    user_id,
                    platform,
                    group_id,
                    session_id,
                    content,
                    memory_type,
                    source,
                    vector,
                    json.dumps(metadata or {}, ensure_ascii=False),
                    float(importance),
                )
            else:
                row = await conn.fetchrow(
                    """
                    INSERT INTO memories (
                        user_id, platform, group_id, session_id, content, memory_type,
                        source, metadata, importance
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
                    RETURNING id
                    """,
                    user_id,
                    platform,
                    group_id,
                    session_id,
                    content,
                    memory_type,
                    source,
                    json.dumps(metadata or {}, ensure_ascii=False),
                    float(importance),
                )
        return str(row["id"])

    async def search_memory(
        self, session_id: str, group_id: str, query_embedding: list[float] | None, limit: int
    ) -> list[Any]:
        if not await self.ensure():
            return []
        async with self.pool.acquire() as conn:
            if query_embedding:
                vector = "[" + ",".join(str(float(x)) for x in query_embedding) + "]"
                return await conn.fetch(
                    """
                    SELECT id, content, memory_type, source, created_at,
                           1 - (embedding <=> $1::vector) AS similarity
                    FROM memories
                    WHERE deleted_at IS NULL
                      AND embedding IS NOT NULL
                      AND (session_id = $2 OR group_id = $3 OR scope = 'global')
                    ORDER BY embedding <=> $1::vector
                    LIMIT $4
                    """,
                    vector,
                    session_id,
                    group_id,
                    limit,
                )
            return await conn.fetch(
                """
                SELECT id, content, memory_type, source, created_at, 0.0 AS similarity
                FROM memories
                WHERE deleted_at IS NULL
                  AND (session_id = $1 OR group_id = $2 OR scope = 'global')
                ORDER BY created_at DESC
                LIMIT $3
                """,
                session_id,
                group_id,
                limit,
            )

    async def list_memories(self, session_id: str | None, limit: int) -> list[Any]:
        if not await self.ensure():
            return []
        async with self.pool.acquire() as conn:
            if session_id:
                return await conn.fetch(
                    """
                    SELECT id, session_id, group_id, memory_type, source, content, created_at
                    FROM memories
                    WHERE deleted_at IS NULL AND session_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    session_id,
                    limit,
                )
            return await conn.fetch(
                """
                SELECT id, session_id, group_id, memory_type, source, content, created_at
                FROM memories
                WHERE deleted_at IS NULL
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )

    async def delete_memory(self, memory_id: str) -> bool:
        if not await self.ensure():
            return False
        async with self.pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE memories SET deleted_at=now(), updated_at=now() WHERE id=$1::uuid AND deleted_at IS NULL",
                memory_id,
            )
        return status.endswith("1")

    async def increment_counter(self, session_id: str) -> int:
        if not await self.ensure():
            return 0
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO message_counters(session_id, counter)
                VALUES($1, 1)
                ON CONFLICT(session_id) DO UPDATE SET
                    counter = message_counters.counter + 1,
                    updated_at = now()
                RETURNING counter
                """,
                session_id,
            )
        return int(row["counter"] or 0)

    async def reset_counter(self, session_id: str) -> None:
        if not await self.ensure():
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO message_counters(session_id, counter, last_summary_at)
                VALUES($1, 0, now())
                ON CONFLICT(session_id) DO UPDATE SET
                    counter=0,
                    last_summary_at=now(),
                    updated_at=now()
                """,
                session_id,
            )

    async def save_analysis(
        self,
        platform: str,
        group_id: str,
        group_name: str,
        result: dict[str, Any],
        count: int,
        start: int | None,
        end: int | None,
    ) -> None:
        if not await self.ensure():
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO analysis_results (
                    platform, group_id, group_name, result_data, message_count,
                    time_range_start, time_range_end
                )
                VALUES($1,$2,$3,$4::jsonb,$5,$6,$7)
                """,
                platform,
                group_id,
                group_name,
                json.dumps(result, ensure_ascii=False),
                int(count),
                start,
                end,
            )


class ContextWindow:
    def __init__(self, max_messages: int):
        self.max_messages = max_messages
        self.data: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.lock = threading.RLock()

    def add(self, session_id: str, role: str, content: str, metadata: dict[str, Any] | None = None) -> None:
        if not content:
            return
        with self.lock:
            history = self.data[session_id]
            history.append(
                {
                    "role": role,
                    "content": content,
                    "time": now_ts(),
                    "metadata": metadata or {},
                }
            )
            if len(history) > self.max_messages:
                del history[: len(history) - self.max_messages]

    def get(self, session_id: str) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.data.get(session_id, []))

    def clear(self, session_id: str) -> None:
        with self.lock:
            self.data[session_id] = []


class AdminPanelThread:
    def __init__(self, plugin: "PgMemoryPlugin"):
        self.plugin = plugin
        self.thread: threading.Thread | None = None
        self.server: Any = None

    def start(self) -> None:
        cfg = self.plugin.cfg("admin_panel", {})
        if not cfg.get("enabled", False):
            return
        if self.thread and self.thread.is_alive():
            return
        try:
            from fastapi import FastAPI, Header, HTTPException
            from fastapi.responses import FileResponse, HTMLResponse
            import uvicorn
        except Exception as exc:
            logger.warning(f"[pg_memory] Admin Panel 依赖不可用: {exc}")
            return

        token = str(cfg.get("token") or DEFAULT_ADMIN)
        app = FastAPI(title="PgMemory Admin Panel")

        async def run_in_plugin_loop(coro):
            plugin_loop = getattr(self.plugin, "_loop", None)
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if not plugin_loop or plugin_loop is current_loop:
                return await coro
            future = asyncio.run_coroutine_threadsafe(coro, plugin_loop)
            return await asyncio.wrap_future(future)

        panel_html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PgMemory Admin</title>
  <style>
    :root {{ color-scheme: light dark; font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    body {{ margin: 0; background: #f5f7fb; color: #18202b; }}
    header {{ padding: 18px 28px; background: #1c2533; color: white; display: flex; justify-content: space-between; gap: 16px; align-items: center; }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 24px; }}
    h1 {{ margin: 0; font-size: 21px; font-weight: 650; }}
    h2 {{ font-size: 17px; margin: 0 0 12px; }}
    .bar {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
    input {{ height: 34px; padding: 0 10px; border: 1px solid #cfd8e3; border-radius: 6px; min-width: 220px; }}
    button {{ height: 36px; border: 0; border-radius: 6px; background: #2f6fed; color: white; padding: 0 14px; cursor: pointer; }}
    button.secondary {{ background: #536173; }}
    section {{ margin-top: 18px; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }}
    .card {{ background: white; border: 1px solid #e0e6ef; border-radius: 8px; padding: 14px; box-shadow: 0 1px 2px rgba(16,24,40,.05); }}
    .metric {{ font-size: 28px; font-weight: 700; margin-top: 4px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #e0e6ef; border-radius: 8px; overflow: hidden; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #edf1f6; text-align: left; vertical-align: top; font-size: 14px; }}
    th {{ background: #eef3fa; font-weight: 650; }}
    .muted {{ color: #627083; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }}
    .truncate {{ max-width: 520px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    @media (max-width: 800px) {{ .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} main {{ padding: 14px; }} }}
  </style>
</head>
<body>
  <header>
    <h1>PgMemory Admin</h1>
    <div class="bar">
      <input id="token" placeholder="管理 Token，默认 {DEFAULT_ADMIN}" />
      <button onclick="saveToken()">保存</button>
      <button class="secondary" onclick="loadAll()">刷新</button>
    </div>
  </header>
  <main>
    <section class="grid" id="stats"></section>
    <section>
      <h2>最近记忆</h2>
      <table><thead><tr><th>ID</th><th>会话</th><th>内容</th><th>时间</th></tr></thead><tbody id="memories"></tbody></table>
    </section>
    <section>
      <h2>最近归档消息</h2>
      <table><thead><tr><th>群号</th><th>发送者</th><th>内容</th><th>时间</th></tr></thead><tbody id="archives"></tbody></table>
    </section>
  </main>
  <script>
    const tokenInput = document.getElementById('token');
    tokenInput.value = localStorage.getItem('pg_memory_token') || '{token}';
    function authHeaders() {{
      const token = (localStorage.getItem('pg_memory_token') || tokenInput.value || '').trim();
      return {{ Authorization: 'Bearer ' + token }};
    }}
    function saveToken() {{
      localStorage.setItem('pg_memory_token', tokenInput.value.trim());
      loadAll();
    }}
    async function getJson(url) {{
      const res = await fetch(url, {{ headers: authHeaders() }});
      if (!res.ok) throw new Error(await res.text());
      return await res.json();
    }}
    function esc(v) {{
      return String(v ?? '').replace(/[&<>"']/g, s => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[s]));
    }}
    async function loadStats() {{
      const data = await getJson('/api/stats');
      document.getElementById('stats').innerHTML = Object.entries(data).map(([k, v]) =>
        `<div class="card"><div class="muted">${{esc(k)}}</div><div class="metric">${{esc(v)}}</div></div>`).join('');
    }}
    async function loadMemories() {{
      const rows = await getJson('/api/memories?limit=80');
      document.getElementById('memories').innerHTML = rows.map(r =>
        `<tr><td class="mono">${{esc(r.id)}}</td><td>${{esc(r.session_id)}}</td><td class="truncate">${{esc(r.content)}}</td><td>${{esc(r.created_at)}}</td></tr>`).join('');
    }}
    async function loadArchives() {{
      const rows = await getJson('/api/archive/messages?limit=80');
      document.getElementById('archives').innerHTML = rows.map(r =>
        `<tr><td class="mono">${{esc(r.group_id)}}</td><td>${{esc(r.sender_name || r.sender_id)}}</td><td class="truncate">${{esc(r.plain_text)}}</td><td>${{esc(r.message_time)}}</td></tr>`).join('');
    }}
    async function loadAll() {{
      try {{ await Promise.all([loadStats(), loadMemories(), loadArchives()]); }}
      catch (err) {{ alert('加载失败：' + err.message); }}
    }}
    loadAll();
  </script>
</body>
</html>"""

        def check_auth(authorization: str = Header(default="")) -> None:
            expected = f"Bearer {token}"
            if token and authorization != expected:
                raise HTTPException(status_code=401, detail="unauthorized")

        @app.get("/", response_class=HTMLResponse)
        @app.get("/dashboard", response_class=HTMLResponse)
        async def dashboard():
            return HTMLResponse(panel_html)

        @app.get("/api/health")
        async def health():
            ok = await run_in_plugin_loop(self.plugin.store.ensure())
            return {"ok": ok, "error": self.plugin.store.last_error}

        @app.get("/reports/{filename}")
        async def report_image(filename: str):
            safe_name = Path(filename).name
            path = self.plugin.report_dir / safe_name
            if not path.exists() or not path.is_file():
                raise HTTPException(status_code=404, detail="not found")
            return FileResponse(path, media_type="image/png")

        @app.get("/api/stats")
        async def stats(_: None = Header(default=None), authorization: str = Header(default="")):
            check_auth(authorization)
            return await run_in_plugin_loop(self.plugin.store.overview())

        @app.get("/api/memories")
        async def memories(limit: int = 50, authorization: str = Header(default="")):
            check_auth(authorization)
            rows = await run_in_plugin_loop(
                self.plugin.store.list_memories(None, clamp_int(limit, 50, 1, 500))
            )
            return [dict(row) for row in rows]

        @app.get("/api/archive/messages")
        async def archive_messages(
            limit: int = 50,
            group_id: str = "",
            authorization: str = Header(default=""),
        ):
            check_auth(authorization)
            if not await run_in_plugin_loop(self.plugin.store.ensure()):
                return []
            safe_limit = clamp_int(limit, 50, 1, 500)
            async def fetch_archive_rows():
                if not self.plugin.store.pool:
                    return []
                if group_id:
                    return await self.plugin.store.pool.fetch(
                        """
                        SELECT group_id, group_name, user_id AS sender_id, user_name AS sender_name,
                               content AS plain_text, time AS message_time
                        FROM archived_messages
                        WHERE group_id = $1
                        ORDER BY time DESC
                        LIMIT $2
                        """,
                        str(group_id),
                        safe_limit,
                    )
                return await self.plugin.store.pool.fetch(
                    """
                    SELECT group_id, group_name, user_id AS sender_id, user_name AS sender_name,
                           content AS plain_text, time AS message_time
                    FROM archived_messages
                    ORDER BY time DESC
                    LIMIT $1
                    """,
                    safe_limit,
                )

            rows = await run_in_plugin_loop(fetch_archive_rows())
            return [dict(row) for row in rows]

        @app.delete("/api/memories/{memory_id}")
        async def delete_memory(memory_id: str, authorization: str = Header(default="")):
            check_auth(authorization)
            deleted = await run_in_plugin_loop(self.plugin.store.delete_memory(memory_id))
            return {"deleted": deleted}

        host = str(cfg.get("host") or "0.0.0.0")
        port = int(cfg.get("port") or 8787)
        config = uvicorn.Config(app, host=host, port=port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        logger.info(f"[pg_memory] Admin Panel 已启动: http://{host}:{port}")

    def stop(self) -> None:
        if self.server:
            self.server.should_exit = True


@register(
    PLUGIN_NAME,
    "axiao-codex",
    "PostgreSQL + pgvector unified archive, memory, and group analysis plugin.",
    "0.1.0",
    "",
)
class PgMemoryPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self.config = config
        self.embedding_dim = self._detect_embedding_dim()
        self.data_dir = self._get_data_dir()
        self.export_dir = self.data_dir / "exports"
        self.report_dir = self.data_dir / "reports"
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        memory_cfg = self.cfg("memory", {})
        max_ctx = clamp_int(memory_cfg.get("max_context_messages"), 80, 10, 1000)
        self.context_window = ContextWindow(max_ctx)
        self.store = PgStore(self)
        self.admin_panel = AdminPanelThread(self)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._archive_task: asyncio.Task | None = None
        self._stop_archive = False
        self._background_tasks: set[asyncio.Task] = set()
        self._start_task(self._startup(), "pg_memory.startup")

    def cfg(self, key: str, default: Any = None) -> Any:
        try:
            val = self.config.get(key, default)
        except Exception:
            return default
        return default if val is None else val

    def _get_data_dir(self) -> Path:
        try:
            return Path(StarTools.get_data_dir())
        except Exception:
            return Path("/AstrBot/data/plugin_data") / PLUGIN_NAME

    def _super_admins(self) -> set[str]:
        raw = self.cfg("super_admins", [DEFAULT_ADMIN])
        admins = {str(item).strip() for item in raw if str(item).strip()} if isinstance(raw, list) else set()
        admins.add(DEFAULT_ADMIN)
        return admins

    def _is_super_admin(self, event: AstrMessageEvent) -> bool:
        return sender_id_of(event) in self._super_admins()

    def _detect_embedding_dim(self) -> int:
        configured_raw = self.cfg("embedding_dim", None)
        if configured_raw is not None:
            return clamp_int(configured_raw, 1536, 1, 65535)
        configured = 1536
        emb_id = str(self.cfg("embedding_provider_id", "") or "").strip()
        try:
            astrbot_cfg = getattr(self.context, "astrbot_config", None)
            providers = astrbot_cfg.get("provider", []) if astrbot_cfg else []
            for provider_cfg in providers:
                if emb_id and str(provider_cfg.get("id") or "") != emb_id:
                    continue
                dim = provider_cfg.get("embedding_dimensions") or provider_cfg.get("embedding_dim")
                if dim:
                    return clamp_int(dim, configured, 1, 65535)
        except Exception:
            pass
        if emb_id:
            return configured
        try:
            provider = self._get_embedding_provider()
            dim = getattr(provider, "embedding_dim", None)
            if not dim and callable(getattr(provider, "get_dim", None)):
                dim = provider.get_dim()
            if dim:
                return int(dim)
        except Exception:
            pass
        return configured

    def _normalize_embedding(self, vector: list[float]) -> list[float]:
        dim = clamp_int(self.embedding_dim, 1536, 1, 65535)
        normalized = [float(x) for x in vector]
        if len(normalized) > dim:
            return normalized[:dim]
        if len(normalized) < dim:
            normalized.extend([0.0] * (dim - len(normalized)))
        return normalized

    def _start_task(self, coro, name: str) -> None:
        try:
            task = asyncio.create_task(coro, name=name)
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        except RuntimeError:
            logger.warning(f"[pg_memory] 无法启动后台任务 {name}")

    async def _startup(self) -> None:
        self._loop = asyncio.get_running_loop()
        await asyncio.sleep(1)
        await self.store.ensure()
        self.admin_panel.start()

    async def terminate(self):
        self._stop_archive = True
        if self._archive_task and not self._archive_task.done():
            self._archive_task.cancel()
        for task in list(self._background_tasks):
            if not task.done():
                task.cancel()
        self.admin_panel.stop()
        await self.store.close()

    def _get_embedding_provider(self) -> Any:
        emb_id = str(self.cfg("embedding_provider_id", "") or "").strip()
        if emb_id:
            try:
                provider = self.context.get_provider_by_id(emb_id)
                if provider:
                    return provider
            except Exception:
                pass
        try:
            providers = self.context.get_all_embedding_providers()
            if providers:
                return providers[0]
        except Exception:
            pass
        return None

    async def _embed(self, text: str) -> list[float] | None:
        provider = self._get_embedding_provider()
        if not provider:
            return None
        safe_text = str(text or "")[:4000]
        try:
            if callable(getattr(provider, "get_embedding", None)):
                result = await provider.get_embedding(safe_text)
            elif callable(getattr(provider, "embed_texts", None)):
                result = await provider.embed_texts([safe_text])
                result = result[0] if result else None
            else:
                return None
            if result and isinstance(result, list):
                return self._normalize_embedding(result)
        except Exception as exc:
            logger.warning(f"[pg_memory] embedding 失败: {exc}")
        return None

    async def _llm_text(self, prompt: str, system_prompt: str = "") -> str:
        try:
            provider = self.context.get_using_provider()
            if not provider:
                return ""
            contexts = [{"role": "system", "content": system_prompt}] if system_prompt else []
            resp = await provider.text_chat(prompt=prompt, contexts=contexts)
            completion = getattr(resp, "completion_text", None) or getattr(resp, "text", None)
            return str(completion or "").strip()
        except Exception as exc:
            logger.warning(f"[pg_memory] LLM 调用失败: {exc}")
            return ""

    async def _send_private_text(self, bot, user_id: str, text: str) -> None:
        if not bot or not user_id:
            return
        for chunk in split_text(text):
            await bot.call_action(
                "send_private_msg",
                user_id=int(user_id),
                message=[{"type": "text", "data": {"text": chunk}}],
            )

    async def _send_group_text(self, bot, group_id: str, text: str) -> None:
        if not bot or not group_id:
            return
        for chunk in split_text(text):
            await bot.call_action(
                "send_group_msg",
                group_id=int(group_id),
                message=[{"type": "text", "data": {"text": chunk}}],
            )

    def _save_report_image(self, image_bytes: bytes) -> Path:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        path = self.report_dir / f"group_analysis_{now_ts()}_{uuid.uuid4().hex}.png"
        path.write_bytes(image_bytes)
        return path

    def _report_image_url(self, path: Path) -> str:
        cfg = self.cfg("admin_panel", {})
        port = int(cfg.get("port") or 8787)
        return f"http://astrbot:{port}/reports/{path.name}"

    async def _send_image_with_fallbacks(
        self,
        bot,
        action: str,
        target_key: str,
        target_value: int,
        image_bytes: bytes,
    ) -> tuple[bool, str]:
        saved_path = self._save_report_image(image_bytes)
        b64 = base64.b64encode(image_bytes).decode("ascii")
        attempts = [
            ("base64", f"base64://{b64}"),
            ("shared-file", f"file://{saved_path.as_posix()}"),
            ("internal-url", self._report_image_url(saved_path)),
        ]
        errors: list[str] = []
        for label, file_value in attempts:
            try:
                await bot.call_action(
                    action,
                    **{
                        target_key: target_value,
                        "message": [{"type": "image", "data": {"file": file_value}}],
                    },
                )
                return True, label
            except Exception as exc:
                errors.append(f"{label}: {exc}")
        return False, " | ".join(errors)

    async def _send_private_image(self, bot, user_id: str, image_bytes: bytes, caption: str = "") -> bool:
        ok, detail = await self._send_image_with_fallbacks(
            bot, "send_private_msg", "user_id", int(user_id), image_bytes
        )
        if ok:
            if caption:
                try:
                    await self._send_private_text(bot, user_id, caption)
                except Exception as exc:
                    logger.warning(f"[pg_memory] private image caption send failed: {exc}")
            logger.info(f"[pg_memory] private image sent by {detail}")
            return True
        logger.warning(f"[pg_memory] private image send failed: {detail}")
        return False

    async def _send_group_image(self, bot, group_id: str, image_bytes: bytes, caption: str = "") -> bool:
        ok, detail = await self._send_image_with_fallbacks(
            bot, "send_group_msg", "group_id", int(group_id), image_bytes
        )
        if ok:
            if caption:
                try:
                    await self._send_group_text(bot, group_id, caption)
                except Exception as exc:
                    logger.warning(f"[pg_memory] group image caption send failed: {exc}")
            logger.info(f"[pg_memory] group image sent by {detail}")
            return True
        logger.warning(f"[pg_memory] group image send failed: {detail}")
        return False

    async def _send_private_file(self, bot, user_id: str, path: Path, caption: str = "") -> bool:
        if not bot or not user_id or not path.exists():
            return False
        if caption:
            try:
                await self._send_private_text(bot, user_id, caption)
            except Exception as exc:
                logger.warning(f"[pg_memory] private file caption send failed: {exc}")
        try:
            await bot.call_action(
                "upload_private_file",
                user_id=int(user_id),
                file=path.as_posix(),
                name=path.name,
            )
            logger.info(f"[pg_memory] private file sent: {path.name}")
            return True
        except Exception as exc:
            logger.warning(f"[pg_memory] private file send failed: {exc}")
            return False

    def _analysis_allowed(self, event: AstrMessageEvent, group_id: str) -> bool:
        if self._is_super_admin(event):
            return True
        cfg = self.cfg("analysis", {})
        mode = str(cfg.get("group_list_mode") or "none").lower()
        values = {str(x).strip() for x in cfg.get("group_list", []) if str(x).strip()}
        keys = {str(group_id), f"{platform_id(event)}:GroupMessage:{group_id}"}
        matched = bool(keys & values)
        if mode == "whitelist":
            return matched
        if mode == "blacklist":
            return not matched
        return True

    @filter.on_astrbot_loaded()
    async def on_astrbot_loaded(self):
        await self.store.ensure()

    @filter.platform_adapter_type(filter.PlatformAdapterType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        await self._passive_archive_message(event)
        if not self.cfg("memory", {}).get("enabled", True):
            return
        text = raw_message_text(event)
        if not text:
            return
        sid = session_id_of(event)
        gid = group_id_of(event)
        self.context_window.add(
            sid,
            "user",
            text,
            {"user_id": sender_id_of(event), "group_id": gid, "platform": platform_id(event)},
        )
        count = await self.store.increment_counter(sid)
        threshold = clamp_int(self.cfg("memory", {}).get("summary_threshold"), 30, 2, 10000)
        if count >= threshold:
            self._start_task(self._summarize_session(sid, gid, sender_id_of(event)), f"pg_memory.summary.{sid}")

    async def _passive_archive_message(self, event: AstrMessageEvent) -> None:
        archive_cfg = self.cfg("archive", {})
        if not archive_cfg.get("passive_enabled", True):
            return
        gid = group_id_of(event)
        if not gid:
            return
        try:
            raw_msg = event_raw_message_dict(event)
            row = build_message_row(platform_id(event), gid, group_name_of(event), raw_msg)
            if not row:
                return
            inserted, _ = await self.store.insert_messages([row])
            if inserted:
                await self.store.refresh_archive_state(platform_id(event), gid, group_name_of(event))
        except Exception as exc:
            logger.warning(f"[pg_memory] 被动归档消息失败: {exc}")

    @filter.on_llm_request()
    async def on_llm_request(self, event: AstrMessageEvent, req: ProviderRequest):
        if not self.cfg("memory", {}).get("enabled", True):
            return
        prompt = req.prompt if isinstance(req.prompt, str) else ""
        if not prompt.strip():
            return
        sid = session_id_of(event)
        gid = group_id_of(event)
        embedding = await self._embed(prompt)
        rows = await self.store.search_memory(
            sid, gid, embedding, clamp_int(self.cfg("memory", {}).get("search_limit"), 5, 1, 20)
        )
        if not rows:
            return
        lines = ["\n[长期记忆检索结果，请作为背景参考，不要逐字复述]"]
        for row in rows:
            sim = row.get("similarity") if hasattr(row, "get") else row["similarity"]
            content = row["content"]
            lines.append(f"- {content} (score={float(sim or 0):.3f})")
        memory_text = "\n".join(lines) + "\n"
        current = req.prompt if isinstance(req.prompt, str) else ""
        req.prompt = memory_text + current

    @filter.on_llm_response()
    async def on_llm_response(self, event: AstrMessageEvent, resp: Any):
        text = str(getattr(resp, "completion_text", "") or getattr(resp, "text", "") or "").strip()
        if text:
            self.context_window.add(session_id_of(event), "assistant", text)

    async def _summarize_session(self, session_id: str, group_id: str, user_id: str) -> None:
        history = self.context_window.get(session_id)
        if not history:
            return
        memory_cfg = self.cfg("memory", {})
        body = "\n".join(
            f"{item['role']}: {item['content']}" for item in history[-clamp_int(memory_cfg.get("max_context_messages"), 80, 10, 1000):]
        )
        prompt = str(memory_cfg.get("long_memory_prompt") or "") + "\n\n" + body
        summary = await self._llm_text(prompt)
        if not summary:
            summary = body[-2000:]
        embedding = await self._embed(summary)
        await self.store.save_memory(
            user_id=user_id,
            platform="onebot",
            group_id=group_id,
            session_id=session_id,
            content=summary,
            memory_type="summary",
            source="auto_summary",
            embedding=embedding,
            metadata={"message_count": len(history)},
        )
        await self.store.reset_counter(session_id)
        self.context_window.clear(session_id)

    @filter.command("归档", alias={"聊天归档", "archive"})
    @filter.permission_type(PermissionType.MEMBER)
    async def archive_cmd(
        self,
        event: AstrMessageEvent,
        target: str = "",
        arg1: str = "",
        arg2: str = "",
        arg3: str = "",
    ):
        if not self._is_super_admin(event):
            yield event.plain_result("权限不足。")
            return
        target = str(target or "").strip()
        if target in {"状态", "status"}:
            yield event.plain_result(await self._format_archive_status())
            return
        if target in {"停止", "stop"}:
            self._stop_archive = True
            yield event.plain_result("已请求停止当前归档任务。")
            return
        if target in {"导出", "export", "整理"}:
            yield event.plain_result(await self._handle_export(event, [arg1, arg2, arg3]))
            return
        if self._archive_task and not self._archive_task.done():
            yield event.plain_result("已有归档任务正在运行。可用 ~归档 状态 查看，或 ~归档 停止 中止。")
            return

        bot = event.bot
        if not bot or not hasattr(bot, "call_action"):
            yield event.plain_result("当前平台不支持 OneBot 调用，无法归档。")
            return
        groups = await self._resolve_archive_groups(event, target)
        if not groups:
            yield event.plain_result("没有可归档的群。群聊中发送 ~归档 可归档当前群；私聊发送 ~归档 all 可归档全部可见群。")
            return
        self._stop_archive = False
        user_id = sender_id_of(event) or DEFAULT_ADMIN
        self._archive_task = asyncio.create_task(self._run_archive_job(bot, platform_id(event), groups, user_id))
        yield event.plain_result(f"已启动归档任务，共 {len(groups)} 个群；进度会私聊发送。")

    async def _resolve_archive_groups(self, event: AstrMessageEvent, target: str) -> list[dict[str, str]]:
        bot = event.bot
        gid = group_id_of(event)
        if not target and gid:
            return [{"group_id": gid, "group_name": gid}]
        if target in {"当前", "current"} and gid:
            return [{"group_id": gid, "group_name": gid}]
        if target in {"all", "全部", "所有"} or (not target and not gid):
            try:
                groups = await bot.call_action("get_group_list")
                return [
                    {
                        "group_id": str(g.get("group_id")),
                        "group_name": str(g.get("group_name") or g.get("group_id")),
                    }
                    for g in (groups if isinstance(groups, list) else [])
                    if g.get("group_id")
                ]
            except Exception as exc:
                logger.warning(f"[pg_memory] 获取群列表失败: {exc}")
                return []
        if re.fullmatch(r"\d{5,}", target):
            return [{"group_id": target, "group_name": await self._resolve_group_name(bot, target) or target}]
        return []

    async def _resolve_group_name(self, bot, group_id: str) -> str:
        try:
            info = await bot.call_action("get_group_info", group_id=int(group_id))
            if isinstance(info, dict):
                return str(info.get("group_name") or "").strip()
        except Exception:
            pass
        return ""

    async def _run_archive_job(self, bot, platform: str, groups: list[dict[str, str]], user_id: str) -> None:
        total = 0
        await self._send_private_text(bot, user_id, f"归档开始，共 {len(groups)} 个群。")
        for idx, group in enumerate(groups, start=1):
            if self._stop_archive:
                break
            gid = group["group_id"]
            name = group.get("group_name") or gid
            await self._send_private_text(bot, user_id, f"[{idx}/{len(groups)}] 开始归档 {name}({gid})")
            stats = await self._archive_one_group(bot, platform, gid, name, user_id)
            total += stats.inserted
            await self._send_private_text(
                bot,
                user_id,
                f"[{idx}/{len(groups)}] 完成 {name}({gid})\n页数:{stats.pages} 拉取:{stats.fetched} 新增:{stats.inserted} 重复:{stats.duplicates}\n范围:{fmt_time(stats.oldest_time)} -> {fmt_time(stats.newest_time)}\n状态:{'到尽头' if stats.exhausted else '未确认尽头'}"
                + (f"\n错误:{stats.error}" if stats.error else ""),
            )
        await self._send_private_text(bot, user_id, f"归档任务结束，新增 {total} 条。\n{await self._format_archive_status(compact=True)}")

    async def _archive_one_group(self, bot, platform: str, group_id: str, group_name: str, user_id: str, stop_before: int | None = None) -> ArchiveStats:
        cfg = self.cfg("archive", {})
        fetch_count = clamp_int(cfg.get("fetch_count"), 100, 10, 100)
        max_pages = clamp_int(cfg.get("max_pages_per_group"), 20000, 1, 1000000)
        progress_every = clamp_int(cfg.get("progress_every_pages"), 5, 1, 1000)
        stats = ArchiveStats(group_id=group_id, group_name=group_name)
        current_anchor: Any = None
        tried: set[str] = set()
        last_anchor = ""
        last_real_seq = 0
        stall = 0
        while stats.pages < max_pages and not self._stop_archive:
            stats.pages += 1
            params: dict[str, Any] = {
                "group_id": int(group_id),
                "count": fetch_count,
                "reverseOrder": True,
                "disable_get_url": True,
                "parse_mult_msg": False,
                "quick_reply": True,
            }
            if current_anchor:
                params["message_seq"] = str(current_anchor)
                tried.add(str(current_anchor))
            try:
                result = await bot.call_action("get_group_msg_history", **params)
            except Exception as exc:
                stats.error = str(exc)
                break
            messages = result.get("messages", []) if isinstance(result, dict) else []
            messages = [msg for msg in messages if isinstance(msg, dict)]
            if stop_before:
                messages = [msg for msg in messages if int(msg.get("time") or 0) >= stop_before]
            if not messages:
                stats.exhausted = True
                break
            stats.fetched += len(messages)
            earliest = min(messages, key=lambda item: int(item.get("time") or 0))
            latest = max(messages, key=lambda item: int(item.get("time") or 0))
            et = int(earliest.get("time") or 0)
            lt = int(latest.get("time") or 0)
            stats.oldest_time = et if not stats.oldest_time else min(stats.oldest_time, et)
            stats.newest_time = max(stats.newest_time, lt)
            rows = [
                row for raw in messages
                if (row := build_message_row(platform, group_id, group_name, raw)) is not None
            ]
            inserted, dup = await self.store.insert_messages(rows)
            stats.inserted += inserted
            stats.duplicates += dup
            candidates = anchor_candidates(earliest)
            next_anchor = candidates[0] if candidates else None
            last_anchor = str(next_anchor or current_anchor or "")
            try:
                last_real_seq = int(earliest.get("real_seq") or last_real_seq or 0)
            except Exception:
                pass
            await self.store.update_archive_state(platform, stats, last_anchor, last_real_seq)
            if stats.pages % progress_every == 0:
                await self._send_private_text(
                    bot,
                    user_id,
                    f"{group_name}({group_id}) 正在归档：第 {stats.pages} 页，新增 {stats.inserted} 条，回溯到 {fmt_time(stats.oldest_time)}",
                )
            if stop_before and et <= stop_before:
                break
            if next_anchor is None:
                stats.exhausted = True
                break
            if current_anchor and str(next_anchor) == str(current_anchor):
                fallback = next((c for c in candidates if str(c) not in tried and str(c) != str(current_anchor)), None)
                if fallback is not None and stall < 8:
                    stall += 1
                    current_anchor = fallback
                    continue
                stats.exhausted = True
                break
            stall = 0
            current_anchor = next_anchor
            await asyncio.sleep(0.08)
        await self.store.update_archive_state(platform, stats, last_anchor, last_real_seq)
        return stats

    async def _format_archive_status(self, compact: bool = False) -> str:
        info = await self.store.overview()
        if info.get("error"):
            return f"PostgreSQL 不可用：{info['error']}"
        lines = [
            "聊天归档状态",
            f"消息数: {info['total']}",
            f"群数: {info['groups']}",
            f"时间范围: {fmt_time(info['oldest'])} -> {fmt_time(info['newest'])}",
            f"任务: {'运行中' if self._archive_task and not self._archive_task.done() else '空闲'}",
        ]
        if not compact and info.get("states"):
            lines.append("")
            lines.append("最近归档群:")
            for row in info["states"][:10]:
                lines.append(
                    f"- {row['group_name']}({row['group_id']}): {row['total_messages']} 条，{fmt_time(row['oldest_time'])} -> {fmt_time(row['newest_time'])}"
                )
        return "\n".join(lines)

    async def _handle_export(self, event: AstrMessageEvent, args: list[str]) -> str:
        fmt, gid, days = self._parse_export_args(args)
        rows = await self.store.fetch_messages(gid, days)
        if not rows:
            return "没有可导出的归档记录。"
        path = self._write_export_file(rows, fmt, gid, days)
        bot = event.bot
        uid = sender_id_of(event)
        if bot and uid:
            try:
                await bot.call_action("upload_private_file", user_id=int(uid), file=str(path), name=path.name)
            except Exception:
                pass
        return f"导出完成：{path}\n格式: {fmt}\n记录数: {len(rows)}"

    def _parse_export_args(self, args: list[str]) -> tuple[str, str | None, int | None]:
        fmt = "txt"
        group_id: str | None = None
        days: int | None = None
        for raw in args:
            token = str(raw or "").strip()
            low = token.lower()
            if not token:
                continue
            if low in {"txt", "text", "md", "markdown", "docx", "word"}:
                fmt = "md" if low == "markdown" else "docx" if low == "word" else low
            elif re.fullmatch(r"\d{5,}", token):
                group_id = token
            else:
                match = re.fullmatch(r"(\d{1,4})(?:天|d|day|days)?", low)
                if match:
                    days = clamp_int(match.group(1), 1, 1, 3650)
        return fmt, group_id, days

    def _write_export_file(self, rows: list[Any], fmt: str, group_id: str | None, days: int | None) -> Path:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        scope = group_id or "all"
        day_part = f"last{days}d" if days else "alltime"
        ext = "docx" if fmt == "docx" else fmt
        path = self.export_dir / f"pg_memory_archive_{scope}_{day_part}_{timestamp}.{ext}"
        lines = self._format_export_lines(rows, markdown=(fmt == "md"))
        if fmt == "docx":
            self._write_docx(path, lines)
        else:
            path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
        return path

    def _format_export_lines(self, rows: list[Any], markdown: bool = False) -> list[str]:
        lines: list[str] = []
        current_group = ""
        current_day = ""
        for row in rows:
            group = f"{row['group_name']}({row['group_id']})"
            if group != current_group:
                lines.extend(["", f"{'# ' if markdown else ''}{group}".strip()])
                current_group = group
                current_day = ""
            day = time.strftime("%Y-%m-%d", time.localtime(int(row["time"])))
            clock = time.strftime("%H:%M:%S", time.localtime(int(row["time"])))
            if day != current_day:
                lines.extend(["", f"{'## ' if markdown else ''}{day}".strip()])
                current_day = day
            content = str(row["content"] or "").replace("\r", " ").replace("\n", " ")
            lines.append(f"[{clock}] {row['user_name']}({row['user_id']}): {content}")
        return lines

    def _write_docx(self, path: Path, lines: list[str]) -> None:
        paragraphs = []
        for line in lines:
            text = html.escape(line)
            paragraphs.append(f'<w:p><w:r><w:t xml:space="preserve">{text}</w:t></w:r></w:p>')
        document_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            "<w:body>"
            + "".join(paragraphs)
            + '<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440"/></w:sectPr>'
            + "</w:body></w:document>"
        )
        content_types = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
            "</Types>"
        )
        rels = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
            "</Relationships>"
        )
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as docx:
            docx.writestr("[Content_Types].xml", content_types)
            docx.writestr("_rels/.rels", rels)
            docx.writestr("word/document.xml", document_xml)

    @filter.command_group("memory")
    def memory_group(self):
        pass

    @memory_group.command("remember")
    async def remember_cmd(self, event: AstrMessageEvent, content: str):
        if not content:
            yield event.plain_result("用法：~memory remember 要记住的内容")
            return
        emb = await self._embed(content)
        mid = await self.store.save_memory(
            user_id=sender_id_of(event),
            platform=platform_id(event),
            group_id=group_id_of(event),
            session_id=session_id_of(event),
            content=content,
            memory_type="manual",
            source="memory_command",
            embedding=emb,
            metadata={},
            importance=0.8,
        )
        yield event.plain_result(f"已保存记忆：{mid}")

    @memory_group.command("list_records")
    async def list_records_cmd(self, event: AstrMessageEvent, limit: int = 10):
        rows = await self.store.list_memories(None if self._is_super_admin(event) else session_id_of(event), clamp_int(limit, 10, 1, 100))
        if not rows:
            yield event.plain_result("暂无记忆记录。")
            return
        lines = ["记忆记录:"]
        for row in rows:
            content = str(row["content"] or "").replace("\n", " ")
            if len(content) > 120:
                content = content[:120] + "..."
            lines.append(f"- {row['id']} [{row['memory_type']}/{row['source']}] {content}")
        yield event.plain_result("\n".join(lines))

    @memory_group.command("list")
    async def list_cmd(self, event: AstrMessageEvent, limit: int = 10):
        async for result in self.list_records_cmd(event, limit):
            yield result

    @memory_group.command("delete_record")
    @filter.permission_type(PermissionType.MEMBER)
    async def delete_record_cmd(self, event: AstrMessageEvent, memory_id: str, confirm: str = ""):
        if not self._is_super_admin(event):
            yield event.plain_result("权限不足。")
            return
        if confirm.lower() not in {"yes", "confirm", "确认"}:
            yield event.plain_result(f"请发送：~memory delete_record {memory_id} confirm")
            return
        ok = await self.store.delete_memory(memory_id)
        yield event.plain_result("已删除。" if ok else "未找到或删除失败。")

    @memory_group.command("delete_session_memory")
    @filter.permission_type(PermissionType.MEMBER)
    async def delete_session_memory_cmd(self, event: AstrMessageEvent, session_id: str = "", confirm: str = ""):
        if not self._is_super_admin(event):
            yield event.plain_result("权限不足。")
            return
        if confirm.lower() not in {"yes", "confirm", "确认"}:
            yield event.plain_result(f"请发送：~memory delete_session_memory {session_id or session_id_of(event)} confirm")
            return
        target = session_id or session_id_of(event)
        rows = await self.store.list_memories(target, 1000)
        deleted = 0
        for row in rows:
            if await self.store.delete_memory(str(row["id"])):
                deleted += 1
        yield event.plain_result(f"已删除会话 {target} 的 {deleted} 条记忆。")

    @memory_group.command("drop_collection")
    @filter.permission_type(PermissionType.MEMBER)
    async def drop_collection_cmd(self, event: AstrMessageEvent, collection_name: str = "default", confirm: str = ""):
        if not self._is_super_admin(event):
            yield event.plain_result("权限不足。")
            return
        yield event.plain_result("新插件使用 PostgreSQL 统一表，不再使用 Milvus collection；如需清理，请使用 delete_session_memory 或 delete_record。")

    @memory_group.command("reset")
    async def reset_session_memory_cmd(self, event: AstrMessageEvent, confirm: str = ""):
        if confirm.lower() not in {"yes", "confirm", "确认"}:
            yield event.plain_result("请发送：~memory reset confirm")
            return
        self.context_window.clear(session_id_of(event))
        await self.store.reset_counter(session_id_of(event))
        yield event.plain_result("当前会话短期上下文已清空。")

    @memory_group.command("get_session_id")
    async def get_session_id_cmd(self, event: AstrMessageEvent):
        yield event.plain_result(session_id_of(event))

    @memory_group.command("init")
    @filter.permission_type(PermissionType.MEMBER)
    async def init_memory_system_cmd(self, event: AstrMessageEvent, force: str = ""):
        if not self._is_super_admin(event):
            yield event.plain_result("权限不足。")
            return
        ok = await self.store.ensure()
        yield event.plain_result(
            f"PostgreSQL 初始化{'成功' if ok else '失败'}。embedding_dim={self.embedding_dim}"
            + (f"\n错误: {self.store.last_error}" if not ok else "")
        )

    @memory_group.command("sync")
    @filter.permission_type(PermissionType.MEMBER)
    async def sync_memory_cmd(self, event: AstrMessageEvent, days: str = "1", max_count: int = 10000):
        if not self._is_super_admin(event):
            yield event.plain_result("权限不足。")
            return
        if str(days).lower() == "all":
            sync_days: int | None = None
        else:
            sync_days = clamp_int(days, 1, 1, 3650)
        gid = group_id_of(event)
        if not gid:
            async for res in self._private_group_select_flow(event, "请选择要同步记忆的群：", lambda ev, group, ds: self._sync_group_memory(ev, group, sync_days, max_count)):
                yield res
            return
        group = {"group_id": gid, "group_name": await self._resolve_group_name(event.bot, gid) or gid}
        count = await self._sync_group_memory(event, group, sync_days, max_count)
        yield event.plain_result(f"同步完成，写入 {count} 条记忆片段。")

    async def _sync_group_memory(self, event: AstrMessageEvent, group: dict[str, str], days: int | None, max_count: int) -> int:
        gid = group["group_id"]
        rows = await self.store.fetch_messages(gid, days, clamp_int(max_count, 10000, 1, 1000000))
        if not rows and event.bot:
            stop_before = now_ts() - days * 86400 if days else None
            await self._archive_one_group(event.bot, platform_id(event), gid, group.get("group_name") or gid, sender_id_of(event) or DEFAULT_ADMIN, stop_before=stop_before)
            rows = await self.store.fetch_messages(gid, days, clamp_int(max_count, 10000, 1, 1000000))
        if not rows:
            return 0
        chunks: list[str] = []
        current: list[str] = []
        for row in rows:
            current.append(f"[{fmt_time(row['time'])}] {row['user_name']}({row['user_id']}): {row['content']}")
            if len(current) >= 80:
                chunks.append("\n".join(current))
                current = []
        if current:
            chunks.append("\n".join(current))
        saved = 0
        prompt_base = str(self.cfg("memory", {}).get("long_memory_prompt") or "")
        for chunk in chunks:
            summary = await self._llm_text(prompt_base + "\n\n" + chunk) or chunk[:2000]
            emb = await self._embed(summary)
            await self.store.save_memory(
                user_id=sender_id_of(event),
                platform=platform_id(event),
                group_id=gid,
                session_id=f"{platform_id(event)}:GroupMessage:{gid}",
                content=summary,
                memory_type="sync_summary",
                source="memory_sync",
                embedding=emb,
                metadata={"group_name": group.get("group_name"), "days": days},
                importance=0.6,
            )
            saved += 1
        return saved

    @filter.command("群分析", alias={"group_analysis"})
    @filter.permission_type(PermissionType.MEMBER)
    async def analyze_group_daily(self, event: AstrMessageEvent, days: int | None = None):
        if not self._is_super_admin(event):
            yield event.plain_result("权限不足。")
            return
        max_days = clamp_int(self.cfg("analysis", {}).get("max_days"), 30, 1, 3650)
        default_days = clamp_int(self.cfg("analysis", {}).get("default_days"), 1, 1, max_days)
        analysis_days = clamp_int(days, default_days, 1, max_days)
        gid = group_id_of(event)
        if not gid:
            async for res in self._private_group_select_flow(
                event,
                f"请选择要分析的群（默认 {analysis_days} 天）：",
                lambda ev, group, selected_days: self._run_group_analysis(ev, group, selected_days or analysis_days, private_user=sender_id_of(event) or DEFAULT_ADMIN),
                allow_days=True,
            ):
                yield res
            return
        if not self._analysis_allowed(event, gid):
            yield event.plain_result("该群未启用群分析。")
            return
        group = {"group_id": gid, "group_name": await self._resolve_group_name(event.bot, gid) or gid}
        yield event.plain_result(f"正在分析最近 {analysis_days} 天消息...")
        await self._run_group_analysis(event, group, analysis_days, private_user="")

    async def _private_group_select_flow(self, event: AstrMessageEvent, title: str, handler, allow_days: bool = False):
        bot = event.bot
        uid = sender_id_of(event) or DEFAULT_ADMIN
        try:
            groups_raw = await bot.call_action("get_group_list")
        except Exception as exc:
            yield event.plain_result(f"获取群列表失败: {exc}")
            return
        groups = [
            {"group_id": str(g.get("group_id")), "group_name": str(g.get("group_name") or g.get("group_id"))}
            for g in (groups_raw if isinstance(groups_raw, list) else [])
            if g.get("group_id")
        ]
        if not groups:
            yield event.plain_result("没有获取到群列表。")
            return
        lines = [title, ""]
        for i, g in enumerate(groups, start=1):
            marker = "" if self._analysis_allowed(event, g["group_id"]) else "（未启用）"
            lines.append(f"{i}. {g['group_name']}（群号：{g['group_id']}）{marker}")
        lines.append("")
        lines.append("回复 ~编号 选择；可回复 ~取消。")
        if allow_days:
            lines.append("也可回复 ~编号 天数，例如：~1 3")
        for chunk in split_text("\n".join(lines)):
            await self._send_private_text(bot, uid, chunk)

        @session_waiter(timeout=120, record_history_chains=False)
        async def waiter(controller: SessionController, selected_event: AstrMessageEvent):
            try:
                selected_event.stop_event()
            except Exception:
                pass
            text = raw_message_text(selected_event).strip()
            if text in {"~取消", "取消"}:
                await self._send_private_text(bot, uid, "已取消。")
                controller.stop()
                return
            m = re.fullmatch(r"~?\s*(\d+)(?:\s+(\d{1,4}|all))?", text, re.I)
            if not m:
                await self._send_private_text(bot, uid, "请输入 ~编号，例如 ~1；或回复 ~取消。")
                return
            idx = int(m.group(1))
            if idx < 1 or idx > len(groups):
                await self._send_private_text(bot, uid, "编号无效，请重新输入。")
                return
            selected_days = None
            if allow_days and m.group(2) and m.group(2).lower() != "all":
                selected_days = clamp_int(m.group(2), 1, 1, clamp_int(self.cfg("analysis", {}).get("max_days"), 30, 1, 3650))
            group = groups[idx - 1]
            if not self._analysis_allowed(event, group["group_id"]):
                await self._send_private_text(bot, uid, f"{group['group_name']}({group['group_id']}) 未启用。")
                controller.stop()
                return
            await self._send_private_text(bot, uid, f"已选择 {group['group_name']}({group['group_id']})，开始处理...")
            result = await handler(selected_event, group, selected_days)
            if isinstance(result, int):
                await self._send_private_text(bot, uid, f"完成，写入 {result} 条。")
            controller.stop()

        try:
            await waiter(event)
        except TimeoutError:
            await self._send_private_text(bot, uid, "选择已超时。")

    async def _run_group_analysis(self, event: AstrMessageEvent, group: dict[str, str], days: int, private_user: str = "") -> None:
        gid = group["group_id"]
        gname = group.get("group_name") or gid
        rows = await self.store.fetch_messages(gid, days)
        if not rows and event.bot:
            await self._archive_one_group(
                event.bot,
                platform_id(event),
                gid,
                gname,
                private_user or sender_id_of(event) or DEFAULT_ADMIN,
                stop_before=now_ts() - days * 86400,
            )
            rows = await self.store.fetch_messages(gid, days)
        if not rows:
            msg = f"群 {gname}({gid}) 最近 {days} 天没有可分析的归档消息。"
            if private_user:
                await self._send_private_text(event.bot, private_user, msg)
            else:
                await self._send_group_text(event.bot, gid, msg)
            return
        report = await self._build_group_report(gid, gname, rows, days)
        await self.store.save_analysis(platform_id(event), gid, gname, {"text": report}, len(rows), int(rows[0]["time"]), int(rows[-1]["time"]))
        prefer_image = bool(self.cfg("analysis", {}).get("prefer_image", True))
        image_bytes = await self._render_report_image(gname, gid, report) if prefer_image else None
        caption = f"📊 来自群 {gid} 的分析报告"
        if private_user:
            if image_bytes:
                if await self._send_private_image(event.bot, private_user, image_bytes, caption):
                    return
                report_image_path = self._save_report_image(image_bytes)
                file_caption = f"{caption}\n私聊图片消息发送失败，已改为 PNG 文件附件发送。"
                if await self._send_private_file(event.bot, private_user, report_image_path, file_caption):
                    return
            await self._send_private_text(event.bot, private_user, caption + "\n" + report)
        else:
            if image_bytes and await self._send_group_image(event.bot, gid, image_bytes, caption):
                return
            await self._send_group_text(event.bot, gid, caption + "\n" + report)

    async def _build_group_report(self, group_id: str, group_name: str, rows: list[Any], days: int) -> str:
        user_counter = Counter(str(row["user_name"] or row["user_id"]) for row in rows)
        hour_counter = Counter(time.strftime("%H", time.localtime(int(row["time"]))) for row in rows)
        sample_lines = [
            f"[{fmt_time(row['time'])}] {row['user_name']}: {row['content']}"
            for row in rows[-300:]
        ]
        prompt = (
            f"请分析群聊 {group_name}({group_id}) 最近 {days} 天的聊天记录，输出中文报告。\n"
            "要求包含：总体摘要、主要话题、活跃成员、重要事件、金句或有趣片段、后续可关注事项。\n\n"
            + "\n".join(sample_lines)
        )
        llm_report = await self._llm_text(prompt)
        stats = [
            f"群：{group_name}（{group_id}）",
            f"时间：最近 {days} 天",
            f"消息数：{len(rows)}",
            f"活跃成员 TOP5：{', '.join(f'{name}({count})' for name, count in user_counter.most_common(5)) or '无'}",
            f"活跃小时 TOP3：{', '.join(f'{hour}:00({count})' for hour, count in hour_counter.most_common(3)) or '无'}",
        ]
        if llm_report:
            return "\n".join(stats) + "\n\n" + llm_report
        topics = Counter()
        for row in rows:
            for word in re.findall(r"[\u4e00-\u9fffA-Za-z0-9_]{2,}", str(row["content"])):
                if word not in {"图片", "表情", "视频", "语音", "这个", "那个", "什么", "哈哈"}:
                    topics[word] += 1
        stats.append(f"高频词：{', '.join(f'{w}({c})' for w, c in topics.most_common(20))}")
        stats.append("\n近期片段：")
        stats.extend(sample_lines[-20:])
        return "\n".join(stats)

    async def _render_report_image(self, group_name: str, group_id: str, report: str) -> bytes | None:
        if not callable(getattr(self, "html_render", None)):
            return None
        html_content = f"""
        <html><head><meta charset="utf-8">
        <style>
        body {{ font-family: "Microsoft YaHei", sans-serif; background: #f5f7fb; margin: 0; padding: 32px; color: #172033; }}
        .page {{ max-width: 900px; margin: 0 auto; background: white; border-radius: 8px; padding: 28px; box-shadow: 0 8px 30px rgba(20,30,60,.12); }}
        h1 {{ font-size: 28px; margin: 0 0 6px; }}
        .sub {{ color: #5b6475; margin-bottom: 22px; }}
        pre {{ white-space: pre-wrap; word-break: break-word; font-size: 16px; line-height: 1.65; }}
        </style></head><body><div class="page">
        <h1>群分析报告</h1><div class="sub">{html.escape(group_name)} ({html.escape(group_id)})</div>
        <pre>{html.escape(report)}</pre>
        </div></body></html>
        """
        try:
            render_options = {
                "type": "browser",
                "quality": 100,
                "device_scale": "2",
                "timeout": 30,
            }
            try:
                rendered = await self.html_render(html_content, {}, False, render_options)
            except TypeError:
                rendered = await self.html_render(html_content, {}, False)
            if isinstance(rendered, bytes):
                return rendered
            if isinstance(rendered, str):
                if rendered.startswith("base64://"):
                    return base64.b64decode(rendered[len("base64://"):])
                path = Path(rendered)
                if path.exists():
                    return path.read_bytes()
        except Exception as exc:
            logger.warning(f"[pg_memory] T2I 渲染失败，回退文本: {exc}")
        return None
