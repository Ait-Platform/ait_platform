# list_includes.py
from pathlib import Path
import re, sys

# Anchor to this script's folder so paths resolve the same everywhere
ROOT = Path(__file__).resolve().parent
TEMPLATES_ROOT = (ROOT / "templates").resolve()

# Match Jinja directives that point to another template via a quoted literal
PATTERN = re.compile(
    r"{%\s*(?:include|import|from|extends)\s+['\"]([^'\"]+)['\"]",
    re.IGNORECASE,
)

def find_refs(template_path: Path, seen=None, depth=0):
    template_path = template_path.resolve()
    if seen is None:
        seen = set()

    if not template_path.exists():
        print("  " * depth + f"⚠ missing: {template_path}")
        return seen

    if template_path in seen:
        print("  " * depth + f"↻ {template_path.name}")
        return seen

    seen.add(template_path)

    # Pretty label (relative to templates/ if possible)
    try:
        label = str(template_path.relative_to(TEMPLATES_ROOT))
    except ValueError:
        label = str(template_path)
    print("  " * depth + f"• {label}")

    text = template_path.read_text(encoding="utf-8", errors="ignore")
    refs = sorted(set(PATTERN.findall(text)))

    for r in refs:
        child = (TEMPLATES_ROOT / r).resolve()
        if child.exists():
            find_refs(child, seen, depth + 1)
        else:
            print("  " * (depth + 1) + f"→ {r}  (not found on disk)")

    return seen

def main():
    if len(sys.argv) < 2:
        print("Usage: python list_includes.py templates\\subject\\loss\\report_pdf.html")
        sys.exit(1)

    start = Path(sys.argv[1])
    # Allow passing either absolute or relative (to templates/)
    if not start.is_absolute():
        start = (TEMPLATES_ROOT / start).resolve()

    find_refs(start)

if __name__ == "__main__":
    main()
