"""
Bronze layer scraper for ExamCademy questions

Modes:
  python scraper.py                # Open Edge, log in with scraper profile, scrape
  python scraper.py --real-profile # Use YOUR real Edge profile (already logged in) ← RECOMMENDED
  python scraper.py --reparse      # Re-parse saved HTML without hitting the site
  python scraper.py --cdp          # Connect to existing Edge via CDP (port 9222)

--real-profile is the most reliable mode:
    - Uses your real Edge profile when possible
  - All your cookies/session are already present — no manual login needed
  - Run: python scraper.py --real-profile
"""

import argparse
import json
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, BrowserContext, Page

# Defaults — all overridden by config.json
_DEFAULT_BASE_URL = "https://examcademy.com"
_DEFAULT_EXAM_PATH = "/exams/microsoft/dp-600"
_DEFAULT_TOTAL_PAGES = 8
AUTH_DOMAIN = "auth.examcademy.com"

BRONS_DIR = Path("brons")
RAW_DIR = BRONS_DIR / "raw"
IMAGES_DIR = BRONS_DIR / "images"
CONFIG_PATH = Path("config.json")
SCRAPER_PROFILE_DIR = Path.home() / ".examcademy_scraper"
EDGE_REAL_PROFILE_DIR = Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data"

_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
if (!window.chrome) { window.chrome = {runtime: {}}; }
"""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _normalize_exam_path(path: str) -> str:
    value = (path or _DEFAULT_EXAM_PATH).strip()
    if not value.startswith("/"):
        value = "/" + value
    return value.rstrip("/")


def _exam_slug(exam_path: str) -> str:
    leaf = _normalize_exam_path(exam_path).split("/")[-1].strip().lower()
    safe = re.sub(r"[^a-z0-9-]+", "-", leaf).strip("-")
    return safe or "exam"


def _exam_display_name(config: dict) -> str:
    if config.get("exam_name"):
        return str(config["exam_name"]).strip()
    return _exam_slug(config.get("exam_path", EXAM_PATH)).upper()

def load_config() -> dict:
    if CONFIG_PATH.exists():
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        for k in ("cookie", "cookies", "_comment", "_how_to_get_cookie"):
            cfg.pop(k, None)
    else:
        cfg = {}

    # Derive exam_path from exam_code (only field required in config.json)
    exam_code = cfg.get("exam_code", "").strip().lower()
    if exam_code and "exam_path" not in cfg:
        cfg["exam_path"] = f"/exams/microsoft/{exam_code}"

    cfg["base_url"] = _DEFAULT_BASE_URL
    cfg["exam_path"] = _normalize_exam_path(cfg.get("exam_path", _DEFAULT_EXAM_PATH))
    cfg["total_pages"] = int(cfg.get("total_pages", 0))  # 0 = auto-detect
    return cfg


# ---------------------------------------------------------------------------
# Playwright helpers
# ---------------------------------------------------------------------------

def _open_scraper_profile(pw) -> BrowserContext:
    SCRAPER_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    ctx = pw.chromium.launch_persistent_context(
        str(SCRAPER_PROFILE_DIR),
        channel="msedge",
        headless=False,
        args=["--disable-blink-features=AutomationControlled"],
        ignore_default_args=["--enable-automation"],
    )
    ctx.add_init_script(_STEALTH_SCRIPT)
    return ctx


def _wait_until_authenticated(page: Page, timeout_s: int = 300) -> None:
    """Block until real (non-skeleton) exam rows appear and paywall is gone."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            # Must have real rows (not just loading skeletons) AND no paywall
            has_real_rows = page.locator("div.exam-row:not(.skeleton)").count() > 0
            no_block = page.locator("div.free-limit-notice").count() == 0
            if has_real_rows and no_block:
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError("Timed out waiting for authenticated exam content.")


# ---------------------------------------------------------------------------
# __next_f parser — correct answers live in server-rendered script tags
# ---------------------------------------------------------------------------

def _extract_next_f_answers(html: str) -> dict[str, str | list]:
    answers: dict = {}
    for raw in re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL):
        blob = raw.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", "\n")
        for m in re.finditer(
            r'"correctAnswer"\s*:\s*"([^"]+)".*?"questionId"\s*:\s*"([a-f0-9]+)"',
            blob,
        ):
            answers[m.group(2)] = m.group(1)
        for m in re.finditer(
            r'"correctAnswer"\s*:\s*(\[[^\]]+\]).*?"questionId"\s*:\s*"([a-f0-9]+)"',
            blob,
        ):
            try:
                answers[m.group(2)] = json.loads(m.group(1))
            except Exception:
                pass
    return answers


def _extract_next_f_choices(html: str) -> dict[str, dict]:
    choices: dict = {}
    for raw in re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL):
        blob = raw.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", "\n")
        for m in re.finditer(
            r'"choices"\s*:\s*(\{[^}]+\}).*?"questionId"\s*:\s*"([a-f0-9]+)"',
            blob,
        ):
            try:
                choices[m.group(2)] = json.loads(m.group(1))
            except Exception:
                pass
    return choices


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def parse_questions(html: str, page_num: int, url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    scraped_at = datetime.now(timezone.utc).isoformat()
    next_answers = _extract_next_f_answers(html)
    next_choices = _extract_next_f_choices(html)
    containers = soup.find_all("div", class_="exam-row")
    if not containers:
        print("  [WARN] No exam-row elements found — scraper protection may be active.")
        return []
    questions = []
    for el in containers:
        q = _build_question(el, page_num, url, scraped_at, next_answers, next_choices)
        if q["question_text"]:
            questions.append(q)
    return questions


def _build_question(el, page_num, url, scraped_at, next_answers, next_choices) -> dict:
    qid = _extract_question_id(el)
    images_question, images_answer = _extract_images_by_section(el)
    images_all = _merge_image_lists(images_question, images_answer)
    question_content = _extract_question_content(el)
    return {
        "question_number": _extract_number(el),
        "question_text": question_content["text"],
        "question_markdown": question_content["markdown"],
        "question_html": question_content["html"],
        "options": next_choices.get(qid) or _extract_options(el),
        "correct_answer": next_answers.get(qid) or _extract_correct_answer(el),
        "explanation": _extract_explanation(el),
        "topic": _extract_topic(el),
        "dropdown_groups": {},
        "available_values": [],
        "statements": [],
        "images": images_all,
        "images_question": images_question,
        "images_answer": images_answer,
        "source_page": page_num,
        "source_url": url,
        "scraped_at": scraped_at,
    }


def _extract_question_id(el) -> str:
    direct = el.get("data-question-id")
    if direct:
        return str(direct)

    tagged = el.find(attrs={"data-question-id": True})
    if tagged and tagged.get("data-question-id"):
        return str(tagged.get("data-question-id"))

    return ""


def _extract_number(el) -> int | None:
    q_id = el.get("id", "")
    if q_id.startswith("q-") and q_id[2:].isdigit():
        return int(q_id[2:])
    h2 = el.find("h2")
    if h2:
        m = re.search(r"(\d+)", h2.get_text())
        return int(m.group(1)) if m else None
    return None


def _extract_question_content(el) -> dict[str, str]:
    content = el.find("div", class_="question-content")
    if not content:
        return {"text": "", "markdown": "", "html": ""}

    html = content.decode_contents().strip()
    markdown_lines: list[str] = []

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
            inner = child.get_text("\n", strip=True).replace("\r", "")
            if inner:
                markdown_lines.append(inner)
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


def _extract_options(el) -> dict:
    options = {}
    mc = el.find("div", class_="mc-question")
    if not mc:
        return options
    for li in mc.find_all("li", class_="mc-option"):
        strong = li.find("strong")
        span = li.find("span")
        if strong and span:
            letter = strong.get_text(strip=True).upper()
            if letter in "ABCDE":
                options[letter] = span.get_text(" ", strip=True)
    return options


def _extract_correct_answer(el) -> str | list | None:
    mc = el.find("div", class_="mc-question")
    if mc:
        def is_marked_correct(li: Tag) -> bool:
            classes = li.get("class", []) or []
            text_classes = " ".join(classes).lower()
            if any(k in text_classes for k in ["correct", "missed", "selected", "answer"]):
                return True
            style = (li.get("style") or "").lower()
            if "font-weight" in style and ("700" in style or "bold" in style):
                return True
            return False

        correct = []
        for li in mc.find_all("li"):
            strong = li.find("strong")
            if strong and is_marked_correct(li):
                letter = strong.get_text(strip=True).upper()
                if letter in "ABCDE":
                    correct.append(letter)

        if len(correct) == 1:
            return correct[0]
        if len(correct) > 1:
            return correct

    # Also check the revealed-content div for text-based answers (non-MC questions
    # where the answer is shown as plain text, e.g. "B" or "Yes / No").
    revealed = el.select_one("div.answer-reveal div.revealed-content")
    if revealed and not revealed.find("img"):
        text = revealed.get_text(" ", strip=True)
        text = re.sub(r"\bHide Answer\b", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"^\s*Answer\s*:\s*", "", text, flags=re.IGNORECASE).strip()
        if text:
            # Single letter → MC-style answer
            if re.fullmatch(r"[A-F]", text.upper()):
                return text.upper()
            return text

    return None


def _expand_all_answers(page: Page) -> None:
    """Click all visible 'Show Answer' buttons so DOM markers are present in saved HTML."""
    try:
        rows = page.locator("div.exam-row:not(.skeleton)")
        row_count = rows.count()
        if row_count == 0:
            return

        clicked = 0

        # Pass 1: walk each question row, force it into view, click row-local Show Answer.
        for i in range(row_count):
            row = rows.nth(i)
            try:
                row.scroll_into_view_if_needed(timeout=3_000)
                page.wait_for_timeout(60)
            except Exception:
                pass

            try:
                btns = row.locator("button:has-text('Show Answer')")
                btn_count = btns.count()
                for j in range(btn_count):
                    try:
                        btn = btns.nth(j)
                        btn.scroll_into_view_if_needed(timeout=2_000)
                        btn.click(timeout=2_500)
                        clicked += 1
                        page.wait_for_timeout(80)
                    except Exception:
                        continue
            except Exception:
                continue

        # Pass 2: cleanup any remaining Show Answer buttons after dynamic updates.
        loops = 0
        while loops < 200:
            loops += 1
            remaining = page.locator("button:has-text('Show Answer')")
            if remaining.count() == 0:
                break
            try:
                btn = remaining.first
                btn.scroll_into_view_if_needed(timeout=2_000)
                btn.click(timeout=2_500)
                clicked += 1
                page.wait_for_timeout(80)
            except Exception:
                try:
                    page.mouse.wheel(0, 1200)
                    page.wait_for_timeout(80)
                except Exception:
                    break

        if clicked:
            print(f"  expanded answers: {clicked} clicks across {row_count} rows")
            page.wait_for_timeout(400)
    except Exception:
        pass


def _extract_explanation(el) -> str | None:
    mc = el.find("div", class_="mc-question")
    if not mc:
        return None
    for pat in [r"explanation", r"rationale", r"answer.text", r"answer-text"]:
        found = mc.find(class_=re.compile(pat, re.I))
        if found:
            return found.get_text(" ", strip=True) or None
    return None


def _extract_topic(el) -> str | None:
    badge = el.find("span", class_="topic-badge")
    return badge.get_text(strip=True) if badge else None


def _collect_img_sources(root: Tag | None) -> list[str]:
    if not root:
        return []
    urls = []
    for img in root.find_all("img"):
        src = img.get("src")
        if src and not src.startswith("data:"):
            urls.append(src)
    return urls


def _dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _merge_image_lists(*lists: list[str]) -> list[str]:
    merged: list[str] = []
    for lst in lists:
        merged.extend(lst)
    return _dedupe_keep_order(merged)


def _extract_images_by_section(el) -> tuple[list[str], list[str]]:
    question_urls: list[str] = []
    answer_urls: list[str] = []

    # Primary question area
    question_content = el.find("div", class_="question-content")
    question_urls.extend(_collect_img_sources(question_content))

    # Option images belong to question content context
    mc = el.find("div", class_="mc-question")
    if mc:
        question_urls.extend(_collect_img_sources(mc))

    # Revealed answer blocks are explicit answer context
    for reveal in el.select("div.answer-reveal div.revealed-content"):
        answer_urls.extend(_collect_img_sources(reveal))

    # Fallback by alt text for edge cases
    for img in el.find_all("img"):
        src = img.get("src")
        if not src or src.startswith("data:"):
            continue
        alt = (img.get("alt") or "").lower()
        if "answer image" in alt or alt.strip() == "answer":
            answer_urls.append(src)

    question_urls = _dedupe_keep_order(question_urls)
    answer_urls = _dedupe_keep_order(answer_urls)

    # Remove any overlap from question list if classified as answer.
    answer_set = set(answer_urls)
    question_urls = [u for u in question_urls if u not in answer_set]

    # Any leftover images in the row default to question section.
    all_urls = _collect_img_sources(el)
    assigned = set(question_urls) | answer_set
    for src in all_urls:
        if src not in assigned:
            question_urls.append(src)

    return question_urls, answer_urls




# ---------------------------------------------------------------------------
# Image download
# ---------------------------------------------------------------------------

def _download_images(questions: list[dict]) -> None:
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    for q in questions:
        q_num = q.get("question_number", "unknown")
        question_urls = q.get("images_question") or []
        answer_urls = q.get("images_answer") or []

        def download_bucket(urls: list[str], section: str) -> list[str]:
            local_paths: list[str] = []
            for idx, src in enumerate(urls):
                ext = Path(src.split("?")[0]).suffix or ".png"
                fname = f"q-{q_num}_{section}_{idx + 1}{ext}"
                fpath = IMAGES_DIR / fname
                try:
                    urllib.request.urlretrieve(src, fpath)
                    local_paths.append(str(fpath).replace("\\", "/"))
                    print(f"    img  -> {fpath.name}")
                except Exception as exc:
                    print(f"    [WARN] image download failed ({src[:60]}): {exc}")
                    local_paths.append(src)
            return local_paths

        question_local = download_bucket(question_urls, "question") if question_urls else []
        answer_local = download_bucket(answer_urls, "answer") if answer_urls else []

        q["images_question"] = question_local
        q["images_answer"] = answer_local
        q["images"] = _merge_image_lists(question_local, answer_local)


# ---------------------------------------------------------------------------
# Core: navigate pages inside the browser and save results
# ---------------------------------------------------------------------------

def scrape_all_browser(page: Page, config: dict) -> None:
    """Navigate all pages in an already-authenticated browser, save HTML + JSON."""
    base_url = config.get("base_url", BASE_URL)
    exam_path = config.get("exam_path", EXAM_PATH)
    total_pages = config.get("total_pages", TOTAL_PAGES)

    BRONS_DIR.mkdir(exist_ok=True)
    RAW_DIR.mkdir(exist_ok=True)

    manifest = {
        "exam": _exam_display_name(config),
        "source": f"{base_url}{exam_path}",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "pages": [],
    }
    total_questions = 0

    page_num = 0
    while True:
        page_num += 1
        url = f"{base_url}{exam_path}/{page_num}"
        page_label = f"{page_num}" if not total_pages else f"{page_num}/{total_pages}"
        print(f"\n[{page_label}]  {url}")

        html = ""
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            # Wait for either real question rows or the paywall to appear
            try:
                page.wait_for_selector(
                    "div.exam-row:not(.skeleton), div.free-limit-notice",
                    timeout=25_000,
                )
            except Exception:
                pass

            if page.locator("div.free-limit-notice").count() > 0:
                print()
                print("=" * 60)
                print(f"  Paywall hit on page {page_num} — you may need to log in.")
                print("  Log in in the Edge window, then press ENTER to retry.")
                print("=" * 60)
                input()
                page.reload(wait_until="domcontentloaded", timeout=30_000)
                try:
                    page.wait_for_selector(
                        "div.exam-row:not(.skeleton)", timeout=25_000
                    )
                except Exception:
                    pass

            _expand_all_answers(page)

            html = page.content()
        except Exception as exc:
            print(f"  [ERROR] Navigation failed: {exc}")

        questions = []
        if html:
            raw_path = RAW_DIR / f"page_{page_num}.html"
            raw_path.write_text(html, encoding="utf-8")
            print(f"  raw  -> {raw_path}  ({len(html):,} bytes)")
            questions = parse_questions(html, page_num, url)
            _download_images(questions)

        page_record = {
            "page": page_num,
            "url": url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "question_count": len(questions),
            "questions": questions,
        }
        json_path = BRONS_DIR / f"page_{page_num}.json"
        json_path.write_text(json.dumps(page_record, indent=2, ensure_ascii=False), encoding="utf-8")
        with_ans = sum(1 for q in questions if q.get("correct_answer"))
        print(f"  json -> {json_path}  ({len(questions)} questions, {with_ans} with answers)")

        manifest["pages"].append({"page": page_num, "url": url, "question_count": len(questions)})
        total_questions += len(questions)

        # Stop when explicit total_pages reached, or no questions found (auto-detect)
        if total_pages and page_num >= total_pages:
            break
        if not total_pages and not questions and page_num > 1:
            json_path.unlink(missing_ok=True)
            print(f"  No questions on page {page_num} — done.")
            page_num -= 1
            break

    manifest["total_questions"] = total_questions
    (BRONS_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"Bronze complete: {total_questions} questions across {page_num} pages")
    print(f"  Raw HTML  ->  {RAW_DIR}/")
    print(f"  JSON      ->  {BRONS_DIR}/page_N.json")


# ---------------------------------------------------------------------------
# Real-profile mode: close Edge, relaunch with actual user profile
# ---------------------------------------------------------------------------

def acquire_and_scrape_real_profile(config: dict) -> None:
    """Launch a controllable Edge window with the real user profile when available."""

    if not EDGE_REAL_PROFILE_DIR.exists():
        print(f"  [ERROR] Real Edge profile not found at: {EDGE_REAL_PROFILE_DIR}")
        print("  Falling back to scraper profile mode.")
        acquire_and_scrape(config)
        return

    print(f"  Launching Edge with real profile: {EDGE_REAL_PROFILE_DIR}")
    with sync_playwright() as pw:
        try:
            ctx = pw.chromium.launch_persistent_context(
                str(EDGE_REAL_PROFILE_DIR),
                channel="msedge",
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
            )
        except Exception as exc:
            print(f"  [ERROR] Could not open controllable Edge with real profile: {exc}")
            print("  Leave your normal Edge windows open and use --cdp if you want to attach to an existing debug session,")
            print("  or close only the locked profile window and retry --real-profile.")
            return
        ctx.add_init_script(_STEALTH_SCRIPT)
        page = ctx.new_page()

        base = config.get("base_url", BASE_URL)
        path = config.get("exam_path", EXAM_PATH)
        page.goto(f"{base}{path}/2", wait_until="domcontentloaded", timeout=30_000)

        try:
            page.wait_for_selector(
                "div.exam-row:not(.skeleton), div.free-limit-notice",
                timeout=20_000,
            )
        except Exception:
            pass

        if page.locator("div.free-limit-notice").count() > 0:
            print()
            print("=" * 60)
            print("  Not logged in. Log in in the Edge window.")
            print("  The scraper continues automatically after login.")
            print("=" * 60)
            _wait_until_authenticated(page)
        else:
            real_rows = page.locator("div.exam-row:not(.skeleton)").count()
            print(f"  Already logged in — {real_rows} real question rows on page 2.")

        scrape_all_browser(page, config)
        ctx.close()


# ---------------------------------------------------------------------------
# Default mode: open scraper profile, wait for login, scrape in-browser
# ---------------------------------------------------------------------------

def acquire_and_scrape(config: dict) -> None:
    base = config.get("base_url", BASE_URL)
    path = config.get("exam_path", EXAM_PATH)

    with sync_playwright() as pw:
        context = _open_scraper_profile(pw)
        page = context.new_page()

        page.goto(f"{base}{path}/2", wait_until="networkidle", timeout=30_000)

        needs_login = (
            AUTH_DOMAIN in page.url
            or page.locator("div.free-limit-notice").count() > 0
        )

        if needs_login:
            print()
            print("=" * 60)
            print("  Log in to examcademy.com in the Edge window.")
            print("  The scraper continues automatically after login.")
            print("=" * 60)
            _wait_until_authenticated(page)
        else:
            print("  Already logged in.")

        scrape_all_browser(page, config)
        context.close()


# ---------------------------------------------------------------------------
# CDP mode: connect to existing user-launched Edge, navigate in-browser
# ---------------------------------------------------------------------------

def scrape_all_cdp(pw, config: dict) -> None:
    base = config.get("base_url", BASE_URL)
    path = config.get("exam_path", EXAM_PATH)

    BRONS_DIR.mkdir(exist_ok=True)
    RAW_DIR.mkdir(exist_ok=True)

    print("  Connecting to Edge on 127.0.0.1:9222 ...")
    browser = pw.chromium.connect_over_cdp("http://127.0.0.1:9222")
    context = browser.contexts[0] if browser.contexts else browser.new_context()
    page = context.new_page()

    print(f"  Navigating to page 2 to verify authentication...")
    page.goto(f"{base}{path}/2", wait_until="networkidle", timeout=30_000)

    if page.locator("div.free-limit-notice").count() > 0:
        print()
        print("=" * 60)
        print("  Not logged in. Log in to examcademy.com in Edge,")
        print("  then press ENTER here to continue.")
        print("=" * 60)
        input()
        page.reload(wait_until="networkidle", timeout=30_000)

    exam_rows = page.locator("div.exam-row").count()
    print(f"  Authentication OK — {exam_rows} exam rows visible on page 2.")
    scrape_all_browser(page, config)


# ---------------------------------------------------------------------------
# Reparse mode
# ---------------------------------------------------------------------------

def reparse_from_html(config: dict) -> None:
    html_files = sorted(RAW_DIR.glob("page_*.html"), key=lambda p: int(p.stem.split("_")[1]))
    if not html_files:
        print(f"No HTML files found in {RAW_DIR}. Run without --reparse first.")
        return

    base_url = config.get("base_url", BASE_URL)
    exam_path = config.get("exam_path", EXAM_PATH)

    total_questions = 0
    for html_path in html_files:
        page_num = int(html_path.stem.split("_")[1])
        url = f"{base_url}{exam_path}/{page_num}"
        html = html_path.read_text(encoding="utf-8")
        questions = parse_questions(html, page_num, url)
        _download_images(questions)
        page_record = {
            "page": page_num,
            "url": url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "question_count": len(questions),
            "questions": questions,
        }
        json_path = BRONS_DIR / f"page_{page_num}.json"
        json_path.write_text(json.dumps(page_record, indent=2, ensure_ascii=False), encoding="utf-8")
        with_ans = sum(1 for q in questions if q.get("correct_answer"))
        print(f"  page {page_num}: {len(questions)} questions, {with_ans} with answers -> {json_path.name}")
        total_questions += len(questions)

    print(f"\nReparsed {total_questions} questions from {len(html_files)} HTML files.")
    print("Run  python pipeline.py  to rebuild silver.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ExamCademy bronze layer scraper")
    parser.add_argument("--real-profile", action="store_true",
                        help="Use your real Edge profile (already logged in) — RECOMMENDED")
    parser.add_argument("--reparse", action="store_true",
                        help="Re-parse saved HTML files without re-scraping")
    parser.add_argument("--cdp", action="store_true",
                        help="Connect to existing Edge via CDP on port 9222")
    args = parser.parse_args()

    config = load_config()

    if args.reparse:
        reparse_from_html(config)
    elif args.cdp:
        with sync_playwright() as pw:
            scrape_all_cdp(pw, config)
    elif args.real_profile:
        acquire_and_scrape_real_profile(config)
    else:
        acquire_and_scrape(config)
