import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_path(base_dir: Path, value: str) -> str:
    if not value:
        return ""
    candidate = Path(value)
    if candidate.is_absolute():
        return str(candidate)
    return str((base_dir / value).resolve())


def _normalize_images(base_dir: Path, values: Any) -> List[str]:
    if not values:
        return []
    return [_resolve_path(base_dir, str(v)) for v in values if v]


def _normalize_correct_answer(raw: Any) -> Dict[str, Any]:
    if raw is None or raw == "":
        return {}

    if isinstance(raw, dict):
        if raw.get("mode"):
            return raw
        if "items" in raw:
            return {"mode": "items", "items": raw.get("items", []), "ordered": bool(raw.get("ordered", False))}
        if "value" in raw:
            return {"mode": "answer", "value": str(raw.get("value") or "")}
        return {"mode": "answer", "value": json.dumps(raw, ensure_ascii=False)}

    if isinstance(raw, list):
        parts = [str(item).strip() for item in raw if str(item).strip()]
        if not parts:
            return {}
        if all(re.fullmatch(r"[A-F]", item.upper()) for item in parts):
            return {"mode": "answer", "value": "".join(item.upper() for item in parts)}
        return {"mode": "answer", "value": " / ".join(parts)}

    return {"mode": "answer", "value": str(raw).strip()}


def _infer_select_count(question_text: str, correct_answer: Dict[str, Any], options: List[Dict[str, str]]) -> int:
    value = str(correct_answer.get("value") or "").strip().upper()
    letter_count = len(re.findall(r"[A-F]", value))
    if letter_count > 1:
        return letter_count
    return 1


def _convert_options(raw_options: Any) -> List[Dict[str, str]]:
    if isinstance(raw_options, dict):
        return [{"key": k, "text": str(v)} for k, v in sorted(raw_options.items())]
    if isinstance(raw_options, list):
        out: List[Dict[str, str]] = []
        for item in raw_options:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or item.get("label") or "").strip()
            text = str(item.get("text") or item.get("value") or "").strip()
            if key:
                out.append({"key": key, "text": text})
        return out
    return []


def _bronze_map(bronze_dir: Path) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    if not bronze_dir.exists():
        return out
    for page_file in sorted(bronze_dir.glob("page_*.json"), key=lambda p: int(p.stem.split("_")[1])):
        page = _read_json(page_file)
        for question in page.get("questions", []):
            qnum = question.get("question_number")
            if qnum is None:
                continue
            try:
                out[int(qnum)] = question
            except Exception:
                continue
    return out


def _question_content_from_tag(content: Any) -> Dict[str, str]:
    if not content:
        return {"text": "", "markdown": "", "html": ""}

    html = content.decode_contents().strip()
    markdown_lines: List[str] = []
    for child in content.children:
        if not getattr(child, "name", None):
            text = str(child).strip()
            if text:
                markdown_lines.append(text)
            continue
        name = child.name.lower()
        if name == "h2":
            markdown_lines.append(f"## {child.get_text(' ', strip=True)}")
        elif name == "p":
            value = child.get_text("\n", strip=True).replace("\r", "")
            if value:
                markdown_lines.append(value)
        elif name in {"ul", "ol"}:
            for li in child.find_all("li", recursive=False):
                value = li.get_text(" ", strip=True)
                if value:
                    markdown_lines.append(f"- {value}")
        elif name == "table":
            for row in child.find_all("tr"):
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
                if cells:
                    markdown_lines.append(" | ".join(cells))
        else:
            value = child.get_text(" ", strip=True)
            if value:
                markdown_lines.append(value)

    markdown = "\n\n".join(line for line in markdown_lines if line).strip()
    text = re.sub(r"\s+", " ", BeautifulSoup(html, "lxml").get_text(" ", strip=True)).strip()
    return {"text": text, "markdown": markdown, "html": html}


def _extract_question_content_from_raw_html(base_dir: Path, source_page: Any, question_number: Any) -> Dict[str, str]:
    if source_page is None or question_number is None:
        return {"text": "", "markdown": "", "html": ""}
    raw_path = base_dir / "brons" / "raw" / f"page_{int(source_page)}.html"
    if not raw_path.exists():
        return {"text": "", "markdown": "", "html": ""}
    soup = BeautifulSoup(raw_path.read_text(encoding="utf-8"), "lxml")
    row = soup.find("div", id=f"q-{int(question_number)}")
    content = row.find("div", class_="question-content") if row else None
    return _question_content_from_tag(content)


def _load_image_fixups(data_root: Path, exam_slug: str) -> Dict[int, Dict[str, Any]]:
    """Load image_fixups.json — written by /analyze-exam-images in Claude Code."""
    fixups_path = data_root / "image_fixups.json"
    if not fixups_path.exists():
        return {}
    try:
        data = json.loads(fixups_path.read_text(encoding="utf-8"))
        raw = data.get(exam_slug, {})
        out: Dict[int, Dict[str, Any]] = {}
        for key, value in raw.items():
            # Keys may be "5", "Q5", etc.
            num_str = re.sub(r"[^0-9]", "", str(key))
            if num_str:
                out[int(num_str)] = value
        return out
    except Exception:
        return {}


def load_exam_questions(data_root: str | Path, exam_slug: str) -> List[Dict[str, Any]]:
    base_dir = Path(data_root).resolve()
    silver_path = base_dir / "silver" / f"{exam_slug}_questions.json"
    bronze_dir = base_dir / "brons"

    silver_questions: List[Dict[str, Any]] = []
    if silver_path.exists():
        silver_data = _read_json(silver_path)
        silver_questions = list(silver_data.get("questions", []))

    bronze_questions = _bronze_map(bronze_dir)
    image_fixups = _load_image_fixups(base_dir, exam_slug)

    source_questions = silver_questions if silver_questions else list(bronze_questions.values())
    merged: List[Dict[str, Any]] = []

    for raw_question in source_questions:
        question = deepcopy(raw_question)
        qnum = question.get("question_number")
        bronze = bronze_questions.get(int(qnum)) if qnum is not None else None

        if bronze:
            for key in (
                "question_text", "question_markdown", "question_html",
                "topic", "options", "correct_answer", "explanation",
                "source_page", "source_url",
                "images", "images_question", "images_answer",
                "dropdown_groups", "available_values", "statements",
            ):
                if not question.get(key) and bronze.get(key) not in (None, "", [], {}):
                    question[key] = deepcopy(bronze.get(key))

        if not question.get("question_html") or not question.get("question_markdown"):
            src_page = question.get("source_page") or (bronze.get("source_page") if bronze else None)
            content = _extract_question_content_from_raw_html(base_dir, src_page, qnum)
            if not question.get("question_text") and content["text"]:
                question["question_text"] = content["text"]
            if not question.get("question_markdown") and content["markdown"]:
                question["question_markdown"] = content["markdown"]
            if not question.get("question_html") and content["html"]:
                question["question_html"] = content["html"]

        # Apply image fixups (generated by /analyze-exam-images in Claude Code)
        question_number = int(qnum) if qnum is not None else None
        fixup = image_fixups.get(question_number or -1, {})
        if fixup:
            for key, value in fixup.items():
                question[key] = deepcopy(value)

        qcode = question.get("qcode") or (f"Q{question_number}" if question_number is not None else "")
        options = _convert_options(question.get("options", {}))
        correct_answer = _normalize_correct_answer(question.get("correct_answer"))
        select_count = _infer_select_count(str(question.get("question_text") or ""), correct_answer, options)

        question["qcode"] = qcode
        question["question_number"] = question_number
        question["options"] = options
        question["correct_answer"] = correct_answer
        question["dropdown_groups"] = deepcopy(question.get("dropdown_groups") or {})
        question["available_values"] = list(question.get("available_values") or [])
        question["statements"] = list(question.get("statements") or [])
        question["select_count"] = select_count

        # Guard: clear obviously wrong correct_answer values that come from bronze parsing
        # errors. Only applied when the fixup did NOT supply a correct_answer.
        if not fixup.get("correct_answer"):
            # Case 1: HOTSPOT items whose values don't appear in their dropdown group
            # (bronze parser extracted HTML value attrs like "lv"/"v" instead of text)
            if correct_answer.get("mode") == "items" and question["dropdown_groups"]:
                dg = question["dropdown_groups"]
                all_values_valid = all(
                    item.get("value") in dg.get(item.get("label"), [])
                    for item in correct_answer.get("items", [])
                )
                if not all_values_valid:
                    question["correct_answer"] = {}
                    correct_answer = {}
            # Case 2: single-letter "answer" mode with no options — misidentified as MC
            # (e.g. a HOTSPOT image question where the image analysis returned "B")
            elif (correct_answer.get("mode") == "answer"
                    and re.fullmatch(r"[A-Fa-f]", str(correct_answer.get("value", "")))
                    and not options
                    and not question["dropdown_groups"]):
                question["correct_answer"] = {}
                correct_answer = {}
            # Case 3: mode=items with no dropdown_groups/available_values/statements
            # and at least one value is clearly garbled (empty, ≤3 chars, or starts
            # with a parsing artifact like "|", ":", "//")
            elif (correct_answer.get("mode") == "items"
                    and not question["dropdown_groups"]
                    and not question["available_values"]
                    and not question["statements"]):
                def _is_garbled(v: str) -> bool:
                    v = (v or "").strip()
                    return (not v or len(v) <= 3
                            or v.startswith("|") or v.startswith(":")
                            or v.startswith("//") or "�" in v)
                if any(_is_garbled(i.get("value", "")) for i in correct_answer.get("items", [])):
                    question["correct_answer"] = {}
                    correct_answer = {}

        # Auto-derive interactive fields from correct_answer for image-only questions
        ca_items = correct_answer.get("items", []) if correct_answer.get("mode") == "items" else []
        if ca_items and not question["dropdown_groups"] and not question["available_values"] and not question["statements"]:
            all_yes_no = all(item.get("value") in ("Yes", "No") for item in ca_items)
            if all_yes_no:
                question["statements"] = [item.get("label", "") for item in ca_items]
            elif correct_answer.get("ordered"):
                question["available_values"] = [item.get("value", "") for item in ca_items]

        question["qtype"] = (
            "HOTSPOT" if question["dropdown_groups"]
            else "DRAGDROP" if question["available_values"]
            else "MULTI" if select_count > 1
            else "MC" if options
            else "TEXT"
        )
        question["images_question"] = _normalize_images(base_dir, question.get("images_question") or [])
        question["images_answer"] = _normalize_images(base_dir, question.get("images_answer") or [])
        question["images"] = _normalize_images(
            base_dir,
            question.get("images") or (question["images_question"] + question["images_answer"])
        )
        question["source_page"] = question.get("source_page") or (bronze.get("source_page") if bronze else None)
        question["source_url"] = question.get("source_url") or (bronze.get("source_url") if bronze else "")
        question["question_text"] = str(question.get("question_text") or "").strip()
        question["question_markdown"] = str(question.get("question_markdown") or "").strip()
        question["question_html"] = str(question.get("question_html") or "").strip()
        question["topic"] = str(question.get("topic") or "").strip()
        question["explanation"] = str(question.get("explanation") or "").strip()

        merged.append(question)

    merged.sort(key=lambda q: (q.get("question_number") or 0, q.get("qcode") or ""))
    return merged
