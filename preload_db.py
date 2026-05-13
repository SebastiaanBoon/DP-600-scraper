"""
Pre-loads OCR question data into the DB and stores the silver mtime so the app
starts instantly without re-running OCR on every restart.
"""
import json
import re
import sys
sys.path.insert(0, 'Practice Exam')
from pathlib import Path
from db import ExamDB
from exam_sources import load_exam_questions

DATA_ROOT = '.'

_config_path = Path('config.json')
if _config_path.exists():
    _cfg = json.loads(_config_path.read_text(encoding='utf-8'))
    _code = _cfg.get('exam_code') or _cfg.get('exam_path', '/exams/microsoft/dp-600').split('/')[-1]
    EXAM_SLUG = re.sub(r'[^a-z0-9-]+', '-', _code.strip().lower()).strip('-') or 'dp-600'
else:
    EXAM_SLUG = 'dp-600'

silver_path = Path(DATA_ROOT).resolve() / 'silver' / f'{EXAM_SLUG}_questions.json'
silver_mtime = str(silver_path.stat().st_mtime)

print('Loading questions via OCR (this may take ~1 minute)...')
loaded = load_exam_questions(DATA_ROOT, EXAM_SLUG)
print(f'Loaded {len(loaded)} questions')

have_answer = sum(1 for q in loaded if q.get('correct_answer'))
print(f'With correct_answer: {have_answer}/{len(loaded)}')

db = ExamDB('Practice Exam/exam_app.db')
stored_slug = db.get_meta('exam_slug') or ''
if db.has_questions() and stored_slug and stored_slug != EXAM_SLUG:
    print(f'Exam changed ({stored_slug} → {EXAM_SLUG}): replacing question bank...')
    db.replace_question_bank(loaded)
else:
    db.upsert_questions(loaded)
db.set_meta('silver_mtime', silver_mtime)
db.set_meta('exam_slug', EXAM_SLUG)
print(f'DB updated. exam_slug={EXAM_SLUG}, silver_mtime cached: {silver_mtime}')

q31 = db.get_question('Q31')
if q31:
    print(f'Q31 correct_answer: {q31["correct_answer"]}')

print('Done! App will now load instantly.')
