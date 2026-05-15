"""
Switch to a different exam, or re-scrape the current one from scratch.

Usage:
    python switch_exam.py az-305   # switch to a new exam
    python switch_exam.py dp-600   # re-scrape the current exam from scratch

Updates config.json and removes stale brons/ and silver/ data.
"""
import json
import re
import shutil
import sys
from pathlib import Path

CONFIG_PATH = Path("config.json")


def main():
    if len(sys.argv) != 2:
        print("Usage: python switch_exam.py <exam-code>  (e.g. az-305)")
        sys.exit(1)

    raw = sys.argv[1].strip().lower()
    exam_code = re.sub(r"[^a-z0-9-]+", "-", raw).strip("-")
    if not exam_code:
        print(f"Invalid exam code: {sys.argv[1]!r}")
        sys.exit(1)

    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8")) if CONFIG_PATH.exists() else {}
    old_code = cfg.get("exam_code", "")

    cfg["exam_code"] = exam_code
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    if old_code == exam_code:
        print(f"config.json: {exam_code!r} (unchanged) — wiping scraped data for a fresh re-scrape")
    else:
        print(f"config.json: {old_code!r} → {exam_code!r}")

    for folder in ("brons", "silver"):
        p = Path(folder)
        if p.exists():
            shutil.rmtree(p)
            print(f"Deleted {folder}/")

    print("\nNext steps:")
    print("  python scraper.py")
    print("  python pipeline.py")
    print("  /analyze-exam-images   (in Claude Code, if needed)")
    print("  python preload_db.py")
    print('  cd "Practice Exam" && streamlit run app.py')


if __name__ == "__main__":
    main()
