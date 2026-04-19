from pathlib import Path
import aiosqlite
import datetime


class DatabaseHandler:
    def __init__(self, db_dir: str):
        self.db_path = Path(db_dir) / "orangevibe.db"

    async def init_db(self):
        async with aiosqlite.connect(self.db_path, timeout=15.0) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS play_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    user_name TEXT,
                    test_id TEXT,
                    result_name TEXT,
                    ai_comment TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS test_stats (
                    test_id TEXT PRIMARY KEY,
                    play_count INTEGER DEFAULT 0
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS create_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.commit()

    async def record_play(
        self,
        user_id: str,
        user_name: str,
        test_id: str,
        result_name: str,
        ai_comment: str,
    ):
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        async with aiosqlite.connect(self.db_path, timeout=15.0) as db:
            await db.execute(
                """
                INSERT INTO play_history (user_id, user_name, test_id, result_name, ai_comment, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (user_id, user_name, test_id, result_name, ai_comment, now_str),
            )
            await db.execute(
                """
                INSERT INTO test_stats (test_id, play_count)
                VALUES (?, 1)
                ON CONFLICT(test_id) DO UPDATE SET play_count = play_count + 1
            """,
                (test_id,),
            )
            await db.commit()

    async def get_hot_quizzes(self, limit: int = 5):
        async with aiosqlite.connect(self.db_path, timeout=15.0) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
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
        """Fetch a list of user_ids who got the identical result_name on the identical test_id, excluding oneself."""
        async with aiosqlite.connect(self.db_path, timeout=15.0) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT DISTINCT user_id FROM play_history WHERE test_id = ? AND result_name = ? AND user_id != ?",
                (test_id, result_name, exclude_user_id),
            ) as cursor:
                rows = await cursor.fetchall()
                return [row["user_id"] for row in rows]

    async def get_user_history(self, user_id: str, test_id: str):
        async with aiosqlite.connect(self.db_path, timeout=15.0) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM play_history WHERE user_id = ? AND test_id = ? ORDER BY created_at DESC LIMIT 1",
                (user_id, test_id),
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def get_daily_create_count(self, user_id: str) -> int:
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.db_path, timeout=15.0) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM create_history WHERE user_id = ? AND date(created_at) = ?",
                (user_id, today),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def record_create(self, user_id: str):
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        async with aiosqlite.connect(self.db_path, timeout=15.0) as db:
            await db.execute(
                "INSERT INTO create_history (user_id, created_at) VALUES (?, ?)",
                (user_id, now_str),
            )
            await db.commit()
