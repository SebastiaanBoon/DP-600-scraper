# Exam Practice Scraper

Scrapes any examcademy.com exam into a local Streamlit practice app.
Change one value in `config.json` to switch exams (DP-600, AZ-305, etc.).

**Current state:** DP-600 fully loaded — 178/178 questions with correct answers.

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| Python 3.11+ | runs the scraper and app |
| Microsoft Edge | authenticated browser for scraping |
| [Claude Code](https://claude.ai/code) personal subscription | analyzes image-only question answers |

---

## Setup (once)

```bash
pip install -r requirements.txt
playwright install msedge
```

`config.json` — the **only** file you ever change:

```json
{
  "exam_code": "dp-600"
}
```

To switch to AZ-305: change `"dp-600"` to `"az-305"`. That's it. Everything else is automatic.

---

## Full pipeline

Run these steps in order when scraping a new exam. For DP-600 only Step 4 and 5 are needed (Steps 1–3 are already done and `image_fixups.json` is committed).

### Step 1 — Scrape

```bash
python scraper.py
```

Opens your real Edge profile (already logged in to examcademy.com). Clicks "Show Answer" on every question, saves HTML + images to `brons/`.

### Step 2 — Build silver layer

```bash
python pipeline.py
```

Merges all bronze pages into `silver/<exam-slug>_questions.json`.

### Step 3 — Extract image-based answers

HOTSPOT and DRAG DROP questions reveal their answers as screenshots — there is no text in the HTML. Claude Code reads those images natively and writes the structured answers to `image_fixups.json`.

> **Skip this step for DP-600** — `image_fixups.json` is already in the repo with all 58 image questions solved.

**When you scrape a new exam, open Claude Code in this folder and run:**

```
/analyze-exam-images
```

Claude Code reads every answer image, identifies what is selected/placed in the UI screenshot, and writes `image_fixups.json`. Takes about 2 minutes per exam.

> **Why Claude Code instead of OCR?**
> You already have the subscription — no extra API key or cost.
> It reads images natively, understands dropdowns/checkboxes/drag-slots visually,
> and produces accurate structured answers.
> No Tesseract, no heavy ML packages, no slow processing.

### Step 4 — Load database

```bash
python preload_db.py
```

Reads the silver layer + `image_fixups.json`, merges them, and writes all questions to `Practice Exam/exam_app.db`. Prints a summary like:

```
Loaded 178 questions
With correct_answer: 178/178
```

### Step 5 — Start the app

```bash
cd "Practice Exam"
streamlit run app.py
```

Open `http://localhost:8501`.

---

## Switching exams

```bash
python switch_exam.py az-305
```

Updates `config.json`, wipes stale `brons/` and `silver/` data, and prints the next steps.
Then run Steps 1–4 as usual. The app detects the exam change and clears the old question bank automatically.

---

## Project layout

```
config.json                ← exam settings (gitignored)
config.example.json        ← template
switch_exam.py             ← switches to a new exam (updates config, wipes stale data)
image_fixups.json          ← image-question answers (written by /analyze-exam-images)
brons/
  raw/page_N.html          ← saved raw HTML per page
  page_N.json              ← parsed questions per page
  images/                  ← downloaded question and answer images
silver/
  <exam-slug>_questions.json
Practice Exam/
  app.py                   ← Streamlit practice app
  exam_app.db              ← SQLite session/answer store (gitignored)
```
