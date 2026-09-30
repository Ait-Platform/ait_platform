"""Read-only, deterministic HOME source inventory. Does not render final manuals."""
import hashlib
import json
from pathlib import Path
from sqlalchemy import text

BUILDER_VERSION = "home-source-snapshot-v1"
FALLBACK_IMAGES = {
    1: "chapter1_observation.jpg", 2: "chapter2_Position.jpg", 3: "chapter3_comparison.jpg",
    4: "chapter4_estimation.jpg", 5: "chapter5_Measurement.jpg", 6: "chapter6_Pattern_Recognition.jpg",
    7: "chapter7_Spatial_Reasoning.jpg", 8: "chapter8_logic.jpg", 9: "chapter9_mathematics.jpg",
    10: "chapter10_critical_thinking.jpg",
}


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def source_snapshot(connection, root):
    root = Path(root).resolve()
    chapters = [dict(r) for r in connection.execute(text(
        "SELECT id,chapter_number,title,objective,image_filename,pass_mark FROM home_chapters "
        "WHERE chapter_number BETWEEN 1 AND 30 ORDER BY chapter_number")).mappings()]
    questions = [dict(r) for r in connection.execute(text(
        "SELECT q.id,q.chapter_id,q.question,q.question_type,q.correct_answer FROM home_questions q "
        "JOIN home_chapters c ON c.id=q.chapter_id WHERE c.chapter_number BETWEEN 1 AND 30 "
        "ORDER BY c.chapter_number,q.id")).mappings()]
    options = [dict(r) for r in connection.execute(text(
        "SELECT o.id,o.question_id,o.option_text,o.sort_order FROM home_question_options o "
        "JOIN home_questions q ON q.id=o.question_id JOIN home_chapters c ON c.id=q.chapter_id "
        "WHERE c.chapter_number BETWEEN 1 AND 30 ORDER BY c.chapter_number,q.id,o.sort_order,o.id")).mappings()]
    files, gaps = {}, []
    paths = ["app/subject_home/routes.py", "app/models/home.py", "templates/subject_home/dashboard.html",
             "templates/subject_home/chapter_db.html"]
    paths += [f"templates/subject_home/chapter{i}_practical.html" for i in range(1, 11)]
    paths += [f"templates/subject_home/chapter{i}_theory.html" for i in range(21, 31)]
    for relative in paths:
        path = root / relative
        if not path.is_file():
            gaps.append("Missing source: " + relative)
            continue
        raw = path.read_bytes()
        files[relative] = dict(sha256=hashlib.sha256(raw).hexdigest(), text=raw.decode("utf-8-sig"))
    by_number = {c["chapter_number"]: c for c in chapters}
    assets = {}
    for number in range(1, 31):
        chapter = by_number.get(number)
        if chapter is None:
            gaps.append(f"Missing chapter {number}")
            continue
        base = ((number - 1) % 10) + 1
        filename = by_number.get(base, {}).get("image_filename") or FALLBACK_IMAGES[base]
        image = (root / "app/static/images" / filename).resolve()
        image_root = (root / "app/static/images").resolve()
        if not image.is_relative_to(image_root) or not image.is_file():
            gaps.append(f"Missing/invalid image for chapter {number}: {filename}")
        else:
            assets["app/static/images/" + filename] = hashlib.sha256(image.read_bytes()).hexdigest()
        chapter_questions = [q for q in questions if q["chapter_id"] == chapter["id"]]
        if number >= 11 and not chapter_questions:
            gaps.append(f"No questions for chapter {number}")
        if number >= 11 and not chapter["objective"]:
            gaps.append(f"Missing objective for chapter {number}")
        for question in chapter_questions:
            qoptions = [o for o in options if o["question_id"] == question["id"]]
            if not question["question"] or not question["correct_answer"] or not qoptions:
                gaps.append(f"Incomplete question {question['id']} in chapter {number}")
            if question["question_type"] not in ("select", "single_select", "multi_select"):
                gaps.append(f"Unsupported rendered question type {question['id']}")
    source = dict(chapters=chapters, questions=questions, options=options, files=files, assets=assets)
    return dict(builder_version=BUILDER_VERSION, source=source,
        source_sha256=hashlib.sha256(canonical(source).encode("utf-8")).hexdigest(), gaps=gaps,
        source_approval=None,
        manual_profiles={
            "participant_manual": {"sequence": [[1,10],[11,20],[21,30]], "teacher_answers": False,
                "expand_theory_review": True, "interactive_controls": "printable responses"},
            "facilitator_manual": {"sequence": [[1,10],[11,20],[21,30]], "teacher_answers": True,
                "expand_theory_review": True, "guidance": "source-linked concise notes; no invented content"},
        })
