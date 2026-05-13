import sqlite3, json

conn = sqlite3.connect('exam_app.db')
conn.row_factory = sqlite3.Row

# Check meta table
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print("Tables:", tables)

if 'meta' in tables:
    meta = conn.execute("SELECT * FROM meta").fetchall()
    print("Meta:", [dict(r) for r in meta])

# Questions missing answers
missing = conn.execute("SELECT qcode, question_number, qtype, correct_answer_json FROM questions WHERE correct_answer_json IS NULL OR correct_answer_json = '{}' ORDER BY question_number").fetchall()
print(f"\nMissing correct_answer: {len(missing)}")
for q in missing:
    print(f"  {q['qcode']} (Q{q['question_number']}) - qtype={q['qtype']} - answer={q['correct_answer_json']}")

# Session count
sessions = conn.execute("SELECT COUNT(*) as c FROM sessions").fetchone()["c"]
print(f"\nSessions preserved: {sessions}")

conn.close()
