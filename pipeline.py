"""
Silver layer pipeline: merges all bronze page files into one consolidated JSON.

Run:
  python pipeline.py

Output:
  silver/dp600_questions.json
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

BRONS_DIR = Path("brons")
SILVER_DIR = Path("silver")
CONFIG_PATH = Path("config.json")
DEFAULT_SOURCE = "https://examcademy.com/exams/microsoft/dp-600"


def _exam_slug_from_source(source_url: str) -> str:
    try:
        path = urlparse(source_url).path.strip("/")
    except Exception:
        path = ""
    slug = path.split("/")[-1] if path else ""
    slug = re.sub(r"[^a-z0-9-]+", "-", slug.lower()).strip("-")
    return slug or "exam"


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def load_bronze_pages() -> list[dict]:
    files = sorted(BRONS_DIR.glob("page_*.json"), key=lambda p: int(p.stem.split("_")[1]))
    if not files:
        raise FileNotFoundError(
            f"No bronze page files found in {BRONS_DIR}/. Run scraper.py first."
        )
    pages = []
    for f in files:
        data = json.loads(f.read_text(encoding="utf-8"))
        pages.append(data)
        print(f"  loaded {f.name}  ({data.get('question_count', 0)} questions)")
    return pages


def _record_fingerprint(question: dict) -> str:
    """Return a stable signature so only truly identical records are deduplicated."""
    payload = {
        "question_number": question.get("question_number"),
        "source_page": question.get("source_page"),
        "source_url": question.get("source_url"),
        "question_text": (question.get("question_text") or "").strip(),
        "options": question.get("options") or {},
        "correct_answer": question.get("correct_answer"),
        "explanation": question.get("explanation"),
        "topic": question.get("topic"),
        "images_question": question.get("images_question") or [],
        "images_answer": question.get("images_answer") or [],
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def merge(pages: list[dict]) -> list[dict]:
    all_questions: list[dict] = []
    seen_records: set[str] = set()

    for page in pages:
        for q in page.get("questions", []):
            fingerprint = _record_fingerprint(q)
            if fingerprint in seen_records:
                print(f"  [SKIP] exact duplicate question_number={q.get('question_number')}")
                continue

            seen_records.add(fingerprint)

            all_questions.append(q)

    all_questions.sort(key=lambda q: (q.get("question_number") or 0))
    return all_questions


def run() -> None:
    SILVER_DIR.mkdir(exist_ok=True)

    print("Loading bronze layer...")
    pages = load_bronze_pages()

    print("\nMerging and deduplicating...")
    questions = merge(pages)

    # Pull metadata from manifest if available
    manifest_path = BRONS_DIR / "manifest.json"
    source_url = DEFAULT_SOURCE
    exam_name = None
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        source_url = manifest.get("source", source_url)
        exam_name = manifest.get("exam")

    if not exam_name:
        cfg = _load_config()
        exam_name = cfg.get("exam_name")

    exam_slug = _exam_slug_from_source(source_url)
    if not exam_name:
        exam_name = exam_slug.upper()

    output = {
        "exam": exam_name,
        "source": source_url,
        "total_questions": len(questions),
        "merged_at": datetime.now(timezone.utc).isoformat(),
        "schema": {
            "question_number": "int — exam question number",
            "question_text": "str — full question body",
            "options": "dict[A-D] — answer option texts",
            "correct_answer": "str|null — correct option letter (null if not revealed in HTML)",
            "explanation": "str|null — explanation text",
            "topic": "str|null — exam topic/domain",
            "images_question": "list[str] — local paths/URLs of images from question and options area",
            "images_answer": "list[str] — local paths/URLs of images from revealed answer area",
            "images": "list[str] — combined question+answer images (backward compatibility)",
            "source_page": "int — page number scraped from",
            "source_url": "str — page URL",
            "scraped_at": "str — ISO 8601 timestamp",
        },
        "questions": questions,
    }

    out_path = SILVER_DIR / f"{exam_slug}_questions.json"
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    # Summary stats
    with_answers = sum(1 for q in questions if q.get("correct_answer"))
    with_explanations = sum(1 for q in questions if q.get("explanation"))
    topics = sorted({q.get("topic") for q in questions if q.get("topic")})

    print(f"\n{'='*60}")
    print(f"Silver complete: {len(questions)} questions → {out_path}")
    print(f"  With correct_answer : {with_answers}/{len(questions)}")
    print(f"  With explanation    : {with_explanations}/{len(questions)}")
    if topics:
        print(f"  Topics ({len(topics)})         : {', '.join(topics)}")

    if with_answers == 0:
        print(
            "\n[HINT] correct_answer is null for all questions.\n"
            "  The 'Show Answer' button on examcademy.com loads answers via JavaScript.\n"
            "  Options to get answers:\n"
            "    A) Use selenium/playwright to click 'Show Answer' on each question.\n"
            "    B) Check if there is an API endpoint (inspect Network tab for XHR calls\n"
            "       when you click 'Show Answer')."
        )


if __name__ == "__main__":
    run()
