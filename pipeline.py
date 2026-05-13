"""
Silver layer pipeline: merges all bronze page files into one consolidated JSON.

Run:
  python pipeline.py

Output:
  silver/dp600_questions.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path

BRONS_DIR = Path("brons")
SILVER_DIR = Path("silver")


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


def merge(pages: list[dict]) -> list[dict]:
    all_questions: list[dict] = []
    seen_numbers: set[int] = set()
    seen_texts: set[str] = set()

    for page in pages:
        for q in page.get("questions", []):
            q_num = q.get("question_number")
            q_text = (q.get("question_text") or "").strip()[:120]

            # Deduplicate by question number (primary) or text snippet (fallback)
            if q_num is not None and q_num in seen_numbers:
                print(f"  [SKIP] duplicate question_number={q_num}")
                continue
            if q_text and q_text in seen_texts:
                print(f"  [SKIP] duplicate text snippet for Q{q_num}")
                continue

            if q_num is not None:
                seen_numbers.add(q_num)
            if q_text:
                seen_texts.add(q_text)

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
    source_url = "https://examcademy.com/exams/microsoft/dp-600"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        source_url = manifest.get("source", source_url)

    output = {
        "exam": "DP-600: Microsoft Fabric Analytics Engineer",
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

    out_path = SILVER_DIR / "dp600_questions.json"
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
