from pathlib import Path
import aiosqlite
import asyncio
import time
import logging

logger = logging.getLogger("astrbot")

# 写操作最大重试次数（应对 SQLITE_BUSY）
_MAX_WRITE_RETRIES = 3
_RETRY_DELAY = 0.5


class DatabaseHandler:
    def __init__(self, db_dir: str):
        self.db_path = Path(db_dir) / "orangevibe.db"
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def init_db(self):
        """初始化数据库，建立持久连接并开启 WAL 模式。"""
        self._conn = await aiosqlite.connect(self.db_path, timeout=15.0)
        self._conn.row_factory = aiosqlite.Row
        # 开启 WAL 日志模式，大幅提升并发读写性能并减少锁冲突
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS play_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                user_name TEXT,
                test_id TEXT,
                result_name TEXT,
                ai_comment TEXT,
                created_at REAL NOT NULL
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS test_stats (
                test_id TEXT PRIMARY KEY,
                play_count INTEGER DEFAULT 0
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS create_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                created_at REAL NOT NULL
            )
        """)
        # 在 created_at 上建索引，加速区间查询
        await self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_create_history_user_date ON create_history (user_id, created_at)"
        )
        await self._conn.commit()

    async def close(self):
        """关闭持久连接（插件卸载时调用）。"""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _exec_write(self, coro_fn):
        """带重试的写操作包装器，应对偶发的 SQLITE_BUSY。"""
        async with self._lock:
            for attempt in range(1, _MAX_WRITE_RETRIES + 1):
                try:
                    await coro_fn()
                    return
                except Exception as e:
                    if "locked" in str(e).lower() and attempt < _MAX_WRITE_RETRIES:
                        logger.warning(
                            f"OrangeVibe DB write retry {attempt}/{_MAX_WRITE_RETRIES}: {e}"
                        )
                        await asyncio.sleep(_RETRY_DELAY * attempt)
                    else:
                        raise

    async def record_play(
        self,
        user_id: str,
        user_name: str,
        test_id: str,
        result_name: str,
        ai_comment: str,
    ):
        now_ts = time.time()

        async def _do():
            await self._conn.execute(
                """
                INSERT INTO play_history (user_id, user_name, test_id, result_name, ai_comment, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, user_name, test_id, result_name, ai_comment, now_ts),
            )
            await self._conn.execute(
                """
                INSERT INTO test_stats (test_id, play_count)
                VALUES (?, 1)
                ON CONFLICT(test_id) DO UPDATE SET play_count = play_count + 1
                """,
                (test_id,),
            )
            await self._conn.commit()

        await self._exec_write(_do)

    async def get_hot_quizzes(self, limit: int = 5):
        async with self._conn.execute(
            "SELECT test_id, play_count FROM test_stats ORDER BY play_count DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {"test_id": row["test_id"], "play_count": row["play_count"]}
                for row in rows
            ]

    async def get_same_result_users(
        self, test_id: str, result_name: str, exclude_user_id: str
    ):
        """Fetch user_ids who got the same result_name on the same test_id, excluding oneself."""
        async with self._conn.execute(
            "SELECT DISTINCT user_id FROM play_history WHERE test_id = ? AND result_name = ? AND user_id != ?",
            (test_id, result_name, exclude_user_id),
        ) as cursor:
            rows = await cursor.fetchall()
            return [row["user_id"] for row in rows]

    async def get_user_history(self, user_id: str, test_id: str):
        async with self._conn.execute(
            "SELECT * FROM play_history WHERE user_id = ? AND test_id = ? ORDER BY created_at DESC LIMIT 1",
            (user_id, test_id),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_daily_create_count(self, user_id: str) -> int:
        # 使用 UNIX 时间戳的数值区间比较，无需依赖字符串字典序
        import datetime

        now = datetime.datetime.now()
        day_start_ts = now.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp()
        day_end_ts = day_start_ts + 86400  # +24h
        async with self._conn.execute(
            "SELECT COUNT(*) FROM create_history WHERE user_id = ? AND created_at >= ? AND created_at < ?",
            (user_id, day_start_ts, day_end_ts),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def record_create(self, user_id: str):
        now_ts = time.time()

        async def _do():
            await self._conn.execute(
                "INSERT INTO create_history (user_id, created_at) VALUES (?, ?)",
                (user_id, now_ts),
            )
            await self._conn.commit()

        await self._exec_write(_do)
