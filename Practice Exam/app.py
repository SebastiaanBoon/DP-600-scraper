import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import streamlit as st

from db import ExamDB
from exam_parser import evaluate_answer
from exam_sources import load_exam_questions


APP_TITLE = "Practice Exam Trainer"
APP_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = APP_ROOT.parent


def _read_config_exam_slug() -> str:
    import json
    config_path = DEFAULT_DATA_ROOT / "config.json"
    if config_path.exists():
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        code = cfg.get("exam_code") or cfg.get("exam_path", "/exams/microsoft/dp-600").split("/")[-1]
        return re.sub(r"[^a-z0-9-]+", "-", code.strip().lower()).strip("-") or "dp-600"
    return "dp-600"


DEFAULT_EXAM_SLUG = _read_config_exam_slug()


def _safe_index(options: List[str], value: str) -> int:
    try:
        return options.index(value)
    except ValueError:
        return 0


def load_questions_if_needed(
    db: ExamDB,
    data_root: str,
    exam_slug: str,
) -> Tuple[bool, str]:
    silver_path = Path(data_root).resolve() / "silver" / f"{exam_slug}_questions.json"
    if not silver_path.exists():
        return False, f"No questions found for slug '{exam_slug}' under {data_root}."

    silver_mtime = str(silver_path.stat().st_mtime)
    if db.has_questions() and db.get_meta("silver_mtime") == silver_mtime:
        return True, "Questions already loaded from database."

    loaded = load_exam_questions(data_root, exam_slug)
    if not loaded:
        return False, f"No questions found for slug '{exam_slug}' under {data_root}."

    stored_slug = db.get_meta("exam_slug") or ""
    if db.has_questions() and stored_slug and stored_slug != exam_slug:
        db.replace_question_bank(loaded)
        msg = f"Switched to {exam_slug.upper()}: loaded {len(loaded)} questions (previous bank cleared)."
    elif db.has_questions():
        db.upsert_questions(loaded)
        msg = f"Refreshed question bank with {len(loaded)} questions."
    else:
        db.upsert_questions(loaded)
        msg = f"Loaded {len(loaded)} questions for {exam_slug}."

    db.set_meta("silver_mtime", silver_mtime)
    db.set_meta("exam_slug", exam_slug)
    return True, msg


def get_status_icon(answer_row: Dict[str, Any]) -> str:
    if not answer_row:
        return "○"
    payload = answer_row.get("answer") or {}
    has_value = any(bool(v) for v in payload.values())
    if not has_value:
        return "○"
    if answer_row.get("is_checked"):
        return "✅" if answer_row.get("is_correct") else "❌"
    return "◐"


def render_question_display(question: Dict[str, Any]) -> None:
    """Display question text with preserved bronze formatting when available."""
    question_number = question.get("question_number") or question.get("qcode", "")
    topic = question.get("topic", "")
    st.markdown(f"### {question_number} — {topic}")

    if question.get("question_html"):
        # Strip embedded <img> tags, Hide Answer buttons, and answer sections from scraped HTML
        clean_html = re.sub(r'<img[^>]*>', '', question["question_html"])
        clean_html = re.sub(r'<button[^>]*>\s*Hide Answer\s*</button>', '', clean_html, flags=re.IGNORECASE)
        clean_html = re.sub(r'<[^>]+>\s*Answer:\s*</[^>]+>', '', clean_html, flags=re.IGNORECASE)
        clean_html = re.sub(r'\bAnswer:\s*', '', clean_html, flags=re.IGNORECASE)
        st.markdown(clean_html, unsafe_allow_html=True)
    elif question.get("question_markdown"):
        clean_md = re.sub(r'\bHide Answer\b', '', question["question_markdown"], flags=re.IGNORECASE)
        clean_md = re.sub(r'\bAnswer:\s*', '', clean_md, flags=re.IGNORECASE)
        st.markdown(clean_md)
    else:
        qtext = question.get("question_text", "")
        for line in qtext.split("\n"):
            if line.strip():
                st.markdown(line)

    question_images = question.get("images_question") or []
    options = question.get("options") or []
    n_image_opts = sum(1 for o in options if str(o.get("text", "")).strip().startswith("!["))
    body_images = question_images[: len(question_images) - n_image_opts] if n_image_opts else question_images
    for img_path in body_images:
        st.image(img_path, width=600)


def render_answer_editor(question: Dict[str, Any], existing_answer: Dict[str, Any], key_prefix: str) -> Dict[str, Any]:
    payload = existing_answer.get("answer", {}) if existing_answer else {}

    options = question.get("options", [])
    dropdown_groups = question.get("dropdown_groups", {})
    statements = question.get("statements", [])
    available_values = question.get("available_values", [])
    qtype = question.get("qtype", "UNKNOWN")

    out: Dict[str, Any] = {}

    # === HOTSPOT / DROPDOWN questions ===
    if dropdown_groups:
        st.markdown("**Select an option for each dropdown:**")
        item_answers = payload.get("item_answers", {})
        for idx, (label, choices) in enumerate(dropdown_groups.items(), start=1):
            # Clean label (e.g., "Dropdown 1 — JOIN type..." → "Dropdown 1: JOIN type...")
            clean_label = label.replace(" — ", ": ")
            available = [""] + choices
            default_val = item_answers.get(label, "")
            chosen = st.selectbox(
                clean_label,
                options=available,
                index=_safe_index(available, default_val),
                key=f"{key_prefix}_dd_{idx}",
            )
            out.setdefault("item_answers", {})[label] = chosen

    # === DRAGDROP / FILL-BLANKS (requires available_values) ===
    elif available_values and question.get("correct_answer", {}).get("mode") == "items":
        item_answers = payload.get("item_answers", {})
        st.markdown("**Select values to fill each position:**")
        blanks = question.get("correct_answer", {}).get("items", [])
        for idx, item in enumerate(blanks, start=1):
            label = item.get("label") or f"Item {idx}"
            opts = [""] + available_values
            default_val = item_answers.get(label, "")
            chosen = st.selectbox(
                f"**{label}**",
                options=opts,
                index=_safe_index(opts, default_val),
                key=f"{key_prefix}_blank_{idx}",
                help=f"Available options: {', '.join(available_values[:3])}..." if len(available_values) > 3 else f"Available: {', '.join(available_values)}",
            )
            out.setdefault("item_answers", {})[label] = chosen

    # === YESNO statements (independent of available_values) ===
    elif statements:
        item_answers = payload.get("item_answers", {})
        st.markdown("**Answer each statement (Yes/No):**")
        for idx, statement in enumerate(statements, start=1):
            # Strip "YES / NO" suffix from statement to match correct_answer labels
            label = statement.replace(" YES / NO", "").strip()
            display_text = statement  # Show full text in UI
            default_val = item_answers.get(label, "")
            chosen = st.radio(
                display_text,
                options=["", "Yes", "No"],
                index=_safe_index(["", "Yes", "No"], default_val),
                horizontal=True,
                key=f"{key_prefix}_yn_{idx}",
            )
            out.setdefault("item_answers", {})[label] = chosen

    # === MULTI-SELECT (multiple choice answers) ===
    elif qtype == "MULTI" or question.get("select_count", 1) > 1:
        if options:
            st.markdown("**Select one or more options:**")
            option_labels = [f"{o['key']}. {o['text']}" for o in options]
            key_to_text = {o["key"]: o["text"] for o in options}

            default_selected = payload.get("selected_options", [])
            selected = st.multiselect(
                "Options",
                options=[o["key"] for o in options],
                default=default_selected,
                key=f"{key_prefix}_multi",
            )
            out["selected_options"] = selected
            out["selected_option_texts"] = [key_to_text.get(s, "") for s in selected]
            
            # Show selected options
            if selected:
                st.info(f"Selected: {' + '.join(selected)}")

    # === SINGLE-SELECT (multiple choice) ===
    elif options:
        st.markdown("**Select one option:**")
        choices = [""] + [o["key"] for o in options]
        key_to_text = {o["key"]: o["text"] for o in options}
        default_key = payload.get("selected_option", "")
        image_opts = all(str(o.get("text", "")).strip().startswith("![") for o in options)

        if image_opts:
            # Options are images — show local images labelled A/B/C/D then radio for selection
            q_images = question.get("images_question") or []
            opt_images = q_images[len(q_images) - len(options):]
            for o, img_path in zip(options, opt_images):
                st.markdown(f"**{o['key']}.**")
                st.image(img_path, use_container_width=True)
            selected = st.radio(
                "Your choice",
                choices,
                index=_safe_index(choices, default_key),
                format_func=lambda x: "Choose..." if x == "" else x,
                key=f"{key_prefix}_single",
                horizontal=True,
            )
        else:
            selected = st.radio(
                "Options",
                choices,
                index=_safe_index(choices, default_key),
                format_func=lambda x: "Choose..." if x == "" else f"{x}. {key_to_text.get(x, '')}",
                key=f"{key_prefix}_single",
            )
        out["selected_option"] = selected
        out["selected_option_text"] = key_to_text.get(selected, "")

    # === FALLBACK TEXT ANSWER ===
    else:
        st.markdown("**Your answer:**")
        text_default = payload.get("text_answer", "")
        out["text_answer"] = st.text_area(
            "Type your answer",
            value=text_default,
            key=f"{key_prefix}_txt",
            placeholder="Type your answer...",
            height=100,
        )

    return out


def submit_and_check_round(
    db: ExamDB,
    session: Dict[str, Any],
    round_row: Dict[str, Any],
    round_questions: List[Dict[str, Any]],
) -> Tuple[int, int, List[str]]:
    round_id = int(round_row["id"])
    answers = db.get_round_answers(round_id)
    total = len(round_questions)
    correct_count = 0
    failed_qcodes: List[str] = []

    for q in round_questions:
        qcode = q["qcode"]
        current = answers.get(qcode, {"answer": {}})
        result = evaluate_answer(q, current.get("answer") or {})
        is_correct = bool(result["is_correct"])
        if is_correct:
            correct_count += 1
        else:
            failed_qcodes.append(qcode)

        db.upsert_answer(
            round_id=round_id,
            qcode=qcode,
            answer_payload=current.get("answer") or {},
            is_checked=True,
            is_correct=is_correct,
            feedback=result.get("feedback", ""),
        )

    db.complete_round(round_id)

    if failed_qcodes:
        next_round = int(session["current_round"]) + 1
        if session["retry_mode"] == "auto":
            db.create_round(int(session["id"]), next_round, failed_qcodes)
            db.update_session_round(int(session["id"]), next_round)
        else:
            db.update_session_round(int(session["id"]), next_round)
    else:
        db.complete_session(int(session["id"]))

    return correct_count, total, failed_qcodes


def render_history(db: ExamDB) -> None:
    st.subheader("📊 Session History")
    sessions = db.list_sessions()
    if not sessions:
        st.info("No sessions yet.")
        return

    for s in sessions:
        rounds = db.list_rounds(int(s["id"]))
        with st.expander(
            f"Session #{s['id']} — {s['name']} ({s['status']}) | {len(rounds)} round(s)",
            expanded=False,
        ):
            for r in rounds:
                questions = db.get_round_questions(int(r["id"]))
                qcodes = [q["qcode"] for q in questions]
                answers = db.get_round_answers(int(r["id"]))
                
                correct_count = sum(
                    1 for q in questions
                    if answers.get(q["qcode"], {}).get("is_correct") is True
                )
                
                col_info, col_action = st.columns([3, 1])
                with col_info:
                    st.markdown(
                        f"**Round {r['round_number']}** | {len(qcodes)} Q | "
                        f"Correct: {correct_count}/{len(qcodes)} | {r['status']} | {r['created_at'][:10]}"
                    )
                with col_action:
                    if r["status"] == "completed":
                        failed_qcodes = [
                            q["qcode"] for q in questions
                            if not (answers.get(q["qcode"], {}).get("is_correct") is True)
                        ]
                        if failed_qcodes:
                            if st.button(
                                f"🔄 Retry {len(failed_qcodes)}",
                                key=f"retry_round_{r['id']}",
                                use_container_width=True,
                            ):
                                next_round_no = int(r["round_number"]) + 1
                                db.create_round(int(s["id"]), next_round_no, failed_qcodes)
                                db.update_session_round(int(s["id"]), next_round_no)
                                st.rerun()


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon="🧠", layout="wide")
    st.title(APP_TITLE)
    st.caption("Local-first practice app with persistent exam sessions, retries, and explanations.")

    db = ExamDB(str(APP_ROOT / "exam_app.db"))

    data_root = str(DEFAULT_DATA_ROOT)
    exam_slug = DEFAULT_EXAM_SLUG
    source_ref = str((Path(data_root).resolve() / "silver" / f"{exam_slug}_questions.json").resolve())

    silver_path = Path(data_root).resolve() / "silver" / f"{exam_slug}_questions.json"
    file_mtime = silver_path.stat().st_mtime if silver_path.exists() else 0
    cache_key = (data_root, exam_slug, file_mtime)
    if st.session_state.get("_questions_cache_key") != cache_key:
        ready, msg = load_questions_if_needed(db, data_root, exam_slug)
        st.session_state["_questions_cache_key"] = cache_key
        st.session_state["_questions_ready"] = ready
        st.session_state["_questions_msg"] = msg
    else:
        ready = st.session_state["_questions_ready"]
        msg = st.session_state["_questions_msg"]

    if ready:
        st.sidebar.success(msg)
    else:
        st.sidebar.error(msg)
        st.stop()

    page = st.sidebar.radio(
        "Navigation",
        ["Practice", "History"],
        index=0,
    )

    if page == "History":
        render_history(db)
        return

    # ==================== PRACTICE PAGE ====================
    st.subheader("Practice")
    sessions = db.list_sessions()
    in_progress_sessions = [s for s in sessions if s["status"] == "in_progress"]

    if "active_session_id" not in st.session_state:
        st.session_state["active_session_id"] = None

    # === START NEW SESSION ===
    with st.expander("➕ Start New Session", expanded=True):
        col_name, col_btn = st.columns([3, 1])
        with col_name:
            default_session_name = f"My {exam_slug.upper()} Session"
            name = st.text_input("Session name", value=default_session_name, key="new_session_name")
        with col_btn:
            if st.button("Create", type="primary", use_container_width=True):
                all_qcodes = db.get_cached_question_codes()
                session_id = db.create_session(name=name, source_docx=source_ref)
                db.create_round(session_id=session_id, round_number=1, qcodes=all_qcodes)
                st.session_state["active_session_id"] = session_id
                st.success(f"✓ Created session #{session_id} with {len(all_qcodes)} questions")
                st.rerun()

    # === CONTINUE SESSION ===
    if in_progress_sessions:
        with st.expander("▶️ Continue Session"):
            for s in in_progress_sessions:
                current_round_no = int(s["current_round"])
                col_info, col_btn = st.columns([4, 1])
                with col_info:
                    st.markdown(
                        f"**#{s['id']} — {s['name']}** | Round {current_round_no} | {s['created_at'][:10]}"
                    )
                with col_btn:
                    if st.button("Open", key=f"open_session_{s['id']}", use_container_width=True):
                        st.session_state["active_session_id"] = s["id"]
                        st.rerun()

    # === NO SESSION SELECTED ===
    if not st.session_state.get("active_session_id"):
        if not in_progress_sessions:
            st.info("No active sessions. Create a new one above.")
        return

    # === ACTIVE SESSION ===
    session_id = int(st.session_state["active_session_id"])
    session = db.get_session(session_id)
    if not session:
        st.warning("Session not found.")
        st.session_state["active_session_id"] = None
        st.rerun()

    st.divider()
    st.markdown(f"### Session #{session['id']} — {session['name']}")

    current_round_no = int(session["current_round"])
    round_row = db.get_round(session_id, current_round_no)

    # === CHECK IF ROUND COMPLETED ===
    if not round_row:
        prev_round_no = current_round_no - 1
        if prev_round_no > 0:
            prev_round = db.get_round(session_id, prev_round_no)
            if prev_round:
                prev_questions = db.get_round_questions(int(prev_round["id"]))
                prev_answers = db.get_round_answers(int(prev_round["id"]))
                failed_qcodes = [
                    q["qcode"]
                    for q in prev_questions
                    if not (prev_answers.get(q["qcode"], {}).get("is_correct") is True)
                ]

                st.markdown(f"#### Round {prev_round_no} Completed")
                correct_count = sum(
                    1
                    for q in prev_questions
                    if prev_answers.get(q["qcode"], {}).get("is_correct") is True
                )
                st.metric("Score", f"{correct_count}/{len(prev_questions)}")

                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("➕ Retry All Questions", type="primary", use_container_width=True):
                        all_qcodes = db.get_question_codes()
                        db.create_round(session_id, current_round_no, all_qcodes)
                        st.rerun()

                with col2:
                    if failed_qcodes:
                        if st.button(
                            f"🔄 Retry Failed ({len(failed_qcodes)})",
                            use_container_width=True,
                        ):
                            db.create_round(session_id, current_round_no, failed_qcodes)
                            st.rerun()
                    else:
                        st.success("✅ All correct!")

                with col3:
                    if st.button("📊 View History", use_container_width=True):
                        st.session_state["view_history"] = True
                        st.rerun()
            else:
                st.info("Session complete. Start a new one or view history.")
        else:
            st.info("No round created yet.")
        return

    # === ACTIVE ROUND QUESTIONS ===
    round_id = int(round_row["id"])
    round_questions = db.get_round_questions(round_id)
    qcodes = [q["qcode"] for q in round_questions]
    answers = db.get_round_answers(round_id)

    stats = db.get_round_stats(round_id, qcodes)

    # === METRICS & NAVIGATION ===
    metric_cols = st.columns(5)
    metric_cols[0].metric("Questions", len(qcodes))
    metric_cols[1].metric("Open", stats["open"])
    metric_cols[2].metric("Filled", stats["filled"])
    metric_cols[3].metric("Checked", stats["checked"])
    metric_cols[4].metric("Correct", stats["correct"])

    st.caption(
        f"Round {round_row['round_number']} | {len(qcodes)} questions"
    )

    # === QUESTION NAVIGATOR ===
    nav_cols = st.columns(10)
    for i, q in enumerate(round_questions):
        icon = get_status_icon(answers.get(q["qcode"], {}))
        with nav_cols[i % 10]:
            if st.button(f"{icon} {q['qcode']}", key=f"nav_{round_id}_{q['qcode']}", use_container_width=True):
                st.session_state["question_idx"] = i

    if "question_idx" not in st.session_state:
        st.session_state["question_idx"] = 0

    idx = int(st.session_state.get("question_idx", 0))
    if idx < 0:
        idx = 0
    if idx >= len(round_questions):
        idx = len(round_questions) - 1

    q = round_questions[idx]
    existing_answer = answers.get(q["qcode"], {})

    # === QUESTION DISPLAY ===
    render_question_display(q)

    # === ANSWER EDITOR ===
    st.divider()
    st.subheader("Your Answer")
    answer_payload = render_answer_editor(q, existing_answer, key_prefix=f"r{round_id}_{q['qcode']}")

    # === ACTION BUTTONS ===
    st.divider()
    col_actions = st.columns(5)
    with col_actions[0]:
        if st.button("✓ Save answer", type="primary", use_container_width=True):
            db.upsert_answer(
                round_id=round_id,
                qcode=q["qcode"],
                answer_payload=answer_payload,
                is_checked=False,
                is_correct=None,
                feedback="",
            )
            st.success("✓ Saved")
            st.rerun()

    with col_actions[1]:
        if st.button("🔍 Check this", use_container_width=True):
            result = evaluate_answer(q, answer_payload)
            db.upsert_answer(
                round_id=round_id,
                qcode=q["qcode"],
                answer_payload=answer_payload,
                is_checked=True,
                is_correct=bool(result["is_correct"]),
                feedback=result.get("feedback", ""),
            )
            st.rerun()

    with col_actions[2]:
        if st.button("← Prev", use_container_width=True):
            st.session_state["question_idx"] = max(0, idx - 1)
            st.rerun()

    with col_actions[3]:
        if st.button("Next →", use_container_width=True):
            st.session_state["question_idx"] = min(len(round_questions) - 1, idx + 1)
            st.rerun()

    with col_actions[4]:
        if st.button("📤 Submit Round", type="primary", use_container_width=True):
            correct_count, total, failed_qcodes = submit_and_check_round(
                db,
                session,
                round_row,
                round_questions,
            )
            st.rerun()

    # === RESULT / FEEDBACK ===
    refreshed_answer = db.get_answer(round_id, q["qcode"])
    if refreshed_answer and refreshed_answer.get("is_checked"):
        st.divider()
        if refreshed_answer.get("is_correct"):
            st.success("✅ **Correct!**")
        else:
            st.error("❌ **Not correct yet.**")

        if refreshed_answer.get("feedback"):
            with st.expander("Check details", expanded=True):
                st.markdown(refreshed_answer["feedback"])

        if q.get("explanation"):
            with st.expander("📖 Explanation", expanded=True):
                st.info(q["explanation"])

        answer_images = q.get("images_answer") or []
        if answer_images:
            st.markdown("**Answer attachment**")
            for img_path in answer_images:
                st.image(img_path, width=600)



if __name__ == "__main__":
    main()
