import json
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional


def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


class ExamDB:
    def __init__(self, db_path: str = "exam_app.db") -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _sort_qcodes_numeric(qcodes: List[str]) -> List[str]:
        def _key(qcode: str) -> int:
            m = re.match(r"^Q(\d+)$", (qcode or "").upper())
            if m:
                return int(m.group(1))
            return 10**9

        return sorted(qcodes, key=lambda q: (_key(q), q))

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS questions (
                    qcode TEXT PRIMARY KEY,
                    question_number INTEGER,
                    topic TEXT,
                    qtype TEXT,
                    question_text TEXT NOT NULL,
                    question_markdown TEXT,
                    question_html TEXT,
                    options_json TEXT,
                    dropdowns_json TEXT,
                    available_values_json TEXT,
                    statements_json TEXT,
                    select_count INTEGER,
                    correct_answer_json TEXT,
                    explanation TEXT,
                    source_page INTEGER,
                    source_url TEXT,
                    images_question_json TEXT,
                    images_answer_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    source_docx TEXT NOT NULL,
                    check_mode TEXT NOT NULL,
                    retry_mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    current_round INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS rounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    round_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id)
                );

                CREATE TABLE IF NOT EXISTS round_questions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id INTEGER NOT NULL,
                    qcode TEXT NOT NULL,
                    order_index INTEGER NOT NULL,
                    FOREIGN KEY (round_id) REFERENCES rounds(id),
                    FOREIGN KEY (qcode) REFERENCES questions(qcode)
                );

                CREATE TABLE IF NOT EXISTS answers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id INTEGER NOT NULL,
                    qcode TEXT NOT NULL,
                    answer_json TEXT,
                    is_checked INTEGER NOT NULL DEFAULT 0,
                    is_correct INTEGER,
                    feedback TEXT,
                    checked_at TEXT,
                    updated_at TEXT NOT NULL,
                    UNIQUE(round_id, qcode),
                    FOREIGN KEY (round_id) REFERENCES rounds(id),
                    FOREIGN KEY (qcode) REFERENCES questions(qcode)
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

            self._ensure_column(conn, "questions", "question_number INTEGER")
            self._ensure_column(conn, "questions", "source_page INTEGER")
            self._ensure_column(conn, "questions", "source_url TEXT")
            self._ensure_column(conn, "questions", "question_markdown TEXT")
            self._ensure_column(conn, "questions", "question_html TEXT")
            self._ensure_column(conn, "questions", "images_question_json TEXT")
            self._ensure_column(conn, "questions", "images_answer_json TEXT")

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column_def: str) -> None:
        column_name = column_def.split()[0]
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")

    def has_questions(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM questions").fetchone()
            return bool(row["c"])

    def get_meta(self, key: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def upsert_questions(self, questions: List[Dict[str, Any]]) -> None:
        now = utc_now()
        with self._connect() as conn:
            for q in questions:
                conn.execute(
                    """
                    INSERT INTO questions (
                        qcode, question_number, topic, qtype, question_text, question_markdown, question_html, options_json, dropdowns_json,
                        available_values_json, statements_json, select_count,
                        correct_answer_json, explanation, source_page, source_url,
                        images_question_json, images_answer_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(qcode) DO UPDATE SET
                        question_number = excluded.question_number,
                        topic = excluded.topic,
                        qtype = excluded.qtype,
                        question_text = excluded.question_text,
                        question_markdown = excluded.question_markdown,
                        question_html = excluded.question_html,
                        options_json = excluded.options_json,
                        dropdowns_json = excluded.dropdowns_json,
                        available_values_json = excluded.available_values_json,
                        statements_json = excluded.statements_json,
                        select_count = excluded.select_count,
                        correct_answer_json = excluded.correct_answer_json,
                        explanation = excluded.explanation,
                        source_page = excluded.source_page,
                        source_url = excluded.source_url,
                        images_question_json = excluded.images_question_json,
                        images_answer_json = excluded.images_answer_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        q["qcode"],
                        q.get("question_number"),
                        q.get("topic", ""),
                        q.get("qtype", "UNKNOWN"),
                        q.get("question_text", ""),
                        q.get("question_markdown", ""),
                        q.get("question_html", ""),
                        json.dumps(q.get("options", []), ensure_ascii=True),
                        json.dumps(q.get("dropdown_groups", {}), ensure_ascii=True),
                        json.dumps(q.get("available_values", []), ensure_ascii=True),
                        json.dumps(q.get("statements", []), ensure_ascii=True),
                        q.get("select_count"),
                        json.dumps(q.get("correct_answer", {}), ensure_ascii=True),
                        q.get("explanation", ""),
                        q.get("source_page"),
                        q.get("source_url", ""),
                        json.dumps(q.get("images_question", []), ensure_ascii=True),
                        json.dumps(q.get("images_answer", []), ensure_ascii=True),
                        now,
                        now,
                    ),
                )

    def replace_question_bank(self, questions: List[Dict[str, Any]]) -> None:
        """Replace all practice data with a fresh question bank.

        This resets prior sessions/rounds/answers so stale question sets do not
        remain after switching source documents.
        """
        now = utc_now()
        with self._connect() as conn:
            conn.execute("DELETE FROM answers")
            conn.execute("DELETE FROM round_questions")
            conn.execute("DELETE FROM rounds")
            conn.execute("DELETE FROM sessions")
            conn.execute("DELETE FROM questions")

            for q in questions:
                conn.execute(
                    """
                    INSERT INTO questions (
                        qcode, question_number, topic, qtype, question_text, question_markdown, question_html, options_json, dropdowns_json,
                        available_values_json, statements_json, select_count,
                        correct_answer_json, explanation, source_page, source_url,
                        images_question_json, images_answer_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        q["qcode"],
                        q.get("question_number"),
                        q.get("topic", ""),
                        q.get("qtype", "UNKNOWN"),
                        q.get("question_text", ""),
                        q.get("question_markdown", ""),
                        q.get("question_html", ""),
                        json.dumps(q.get("options", []), ensure_ascii=True),
                        json.dumps(q.get("dropdown_groups", {}), ensure_ascii=True),
                        json.dumps(q.get("available_values", []), ensure_ascii=True),
                        json.dumps(q.get("statements", []), ensure_ascii=True),
                        q.get("select_count"),
                        json.dumps(q.get("correct_answer", {}), ensure_ascii=True),
                        q.get("explanation", ""),
                        q.get("source_page"),
                        q.get("source_url", ""),
                        json.dumps(q.get("images_question", []), ensure_ascii=True),
                        json.dumps(q.get("images_answer", []), ensure_ascii=True),
                        now,
                        now,
                    ),
                )

    def initialize_question_bank(self, questions: List[Dict[str, Any]]) -> None:
        """Initialize the question bank only if it is empty."""
        if not self.has_questions():
            self.replace_question_bank(questions)

    def get_questions(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM questions
                ORDER BY COALESCE(question_number, CAST(SUBSTR(qcode, 2) AS INTEGER)), qcode
                """
            ).fetchall()
            return [self._row_to_question(r) for r in rows]

    def get_question(self, qcode: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM questions WHERE qcode = ?", (qcode,)).fetchone()
            return self._row_to_question(row) if row else None

    def _row_to_question(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "qcode": row["qcode"],
            "question_number": row["question_number"],
            "topic": row["topic"],
            "qtype": row["qtype"],
            "question_text": row["question_text"],
            "question_markdown": row["question_markdown"] or "",
            "question_html": row["question_html"] or "",
            "options": json.loads(row["options_json"] or "[]"),
            "dropdown_groups": json.loads(row["dropdowns_json"] or "{}"),
            "available_values": json.loads(row["available_values_json"] or "[]"),
            "statements": json.loads(row["statements_json"] or "[]"),
            "select_count": row["select_count"],
            "correct_answer": json.loads(row["correct_answer_json"] or "{}"),
            "explanation": row["explanation"] or "",
            "source_page": row["source_page"],
            "source_url": row["source_url"] or "",
            "images_question": json.loads(row["images_question_json"] or "[]"),
            "images_answer": json.loads(row["images_answer_json"] or "[]"),
        }

    def create_session(self, name: str, source_docx: str) -> int:
        now = utc_now()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO sessions (
                    name, source_docx, check_mode, retry_mode, status,
                    current_round, created_at, updated_at
                ) VALUES (?, ?, 'flexible', 'manual', 'in_progress', 1, ?, ?)
                """,
                (name, source_docx, now, now),
            )
            return int(cur.lastrowid)

    def list_sessions(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM sessions ORDER BY updated_at DESC, id DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_session(self, session_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
            return dict(row) if row else None

    def update_session_round(self, session_id: int, current_round: int) -> None:
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE sessions SET current_round = ?, updated_at = ? WHERE id = ?",
                (current_round, now, session_id),
            )

    def complete_session(self, session_id: int) -> None:
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE sessions
                SET status = 'completed', completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, now, session_id),
            )

    def create_round(self, session_id: int, round_number: int, qcodes: List[str]) -> int:
        now = utc_now()
        ordered_qcodes = self._sort_qcodes_numeric(qcodes)
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO rounds (session_id, round_number, status, created_at)
                VALUES (?, ?, 'in_progress', ?)
                """,
                (session_id, round_number, now),
            )
            round_id = int(cur.lastrowid)

            # Bulk insert for better performance
            conn.executemany(
                """
                INSERT INTO round_questions (round_id, qcode, order_index)
                VALUES (?, ?, ?)
                """,
                [(round_id, qcode, idx) for idx, qcode in enumerate(ordered_qcodes, 1)],
            )

            return round_id

    def get_round(self, session_id: int, round_number: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM rounds
                WHERE session_id = ? AND round_number = ?
                LIMIT 1
                """,
                (session_id, round_number),
            ).fetchone()
            return dict(row) if row else None

    def get_current_round(self, session_id: int) -> Optional[Dict[str, Any]]:
        session = self.get_session(session_id)
        if not session:
            return None
        return self.get_round(session_id, int(session["current_round"]))

    def list_rounds(self, session_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM rounds WHERE session_id = ? ORDER BY round_number ASC",
                (session_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def complete_round(self, round_id: int) -> None:
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE rounds SET status = 'completed', completed_at = ? WHERE id = ?",
                (now, round_id),
            )

    def get_round_questions(self, round_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT rq.qcode
                FROM round_questions rq
                WHERE rq.round_id = ?
                ORDER BY CAST(SUBSTR(rq.qcode, 2) AS INTEGER), rq.qcode
                """,
                (round_id,),
            ).fetchall()
            qcodes = [r["qcode"] for r in rows]
            questions = []
            for qcode in qcodes:
                q = self.get_question(qcode)
                if q:
                    questions.append(q)
            return questions

    def upsert_answer(
        self,
        round_id: int,
        qcode: str,
        answer_payload: Dict[str, Any],
        is_checked: bool = False,
        is_correct: Optional[bool] = None,
        feedback: Optional[str] = None,
    ) -> None:
        now = utc_now()
        checked_at = now if is_checked else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO answers (
                    round_id, qcode, answer_json, is_checked,
                    is_correct, feedback, checked_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(round_id, qcode) DO UPDATE SET
                    answer_json = excluded.answer_json,
                    is_checked = excluded.is_checked,
                    is_correct = excluded.is_correct,
                    feedback = excluded.feedback,
                    checked_at = excluded.checked_at,
                    updated_at = excluded.updated_at
                """,
                (
                    round_id,
                    qcode,
                    json.dumps(answer_payload, ensure_ascii=True),
                    1 if is_checked else 0,
                    None if is_correct is None else (1 if is_correct else 0),
                    feedback,
                    checked_at,
                    now,
                ),
            )

    def get_answer(self, round_id: int, qcode: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM answers WHERE round_id = ? AND qcode = ?",
                (round_id, qcode),
            ).fetchone()
            if not row:
                return None
            return {
                "answer": json.loads(row["answer_json"] or "{}"),
                "is_checked": bool(row["is_checked"]),
                "is_correct": None
                if row["is_correct"] is None
                else bool(row["is_correct"]),
                "feedback": row["feedback"] or "",
            }

    def get_round_answers(self, round_id: int) -> Dict[str, Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM answers WHERE round_id = ?",
                (round_id,),
            ).fetchall()
            out: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                out[row["qcode"]] = {
                    "answer": json.loads(row["answer_json"] or "{}"),
                    "is_checked": bool(row["is_checked"]),
                    "is_correct": None
                    if row["is_correct"] is None
                    else bool(row["is_correct"]),
                    "feedback": row["feedback"] or "",
                }
            return out

    def get_round_stats(self, round_id: int, qcodes: List[str]) -> Dict[str, int]:
        answers = self.get_round_answers(round_id)
        open_count = 0
        filled = 0
        checked = 0
        correct = 0
        wrong = 0
        for qcode in qcodes:
            a = answers.get(qcode)
            if not a:
                open_count += 1
                continue
            payload = a.get("answer") or {}
            has_value = any(bool(v) for v in payload.values())
            if has_value:
                filled += 1
            else:
                open_count += 1
            if a.get("is_checked"):
                checked += 1
                if a.get("is_correct"):
                    correct += 1
                elif a.get("is_correct") is False:
                    wrong += 1
        return {
            "open": open_count,
            "filled": filled,
            "checked": checked,
            "correct": correct,
            "wrong": wrong,
        }

    def get_question_codes(self) -> List[str]:
        """Fetch only question codes for performance optimization."""
        with self._connect() as conn:
            rows = conn.execute("SELECT qcode FROM questions ORDER BY qcode").fetchall()
            return [row["qcode"] for row in rows]

    def prepare_question_codes(self) -> None:
        """Prepare and cache question codes during the setup stage."""
        self._cached_qcodes = self.get_question_codes()

    def get_cached_question_codes(self) -> List[str]:
        """Retrieve cached question codes, ensuring they are prepared first."""
        if not hasattr(self, "_cached_qcodes"):
            self.prepare_question_codes()
        return self._cached_qcodes
