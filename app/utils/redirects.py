def safe_next(next_url: str | None, *, default: str = "/") -> str:
    """
    Allow ONLY local relative paths (same-site).
    Always returns a usable path string.
    """
    nxt = (next_url or "").strip()
    if not nxt:
        return default
    if nxt.startswith(("http://", "https://", "//")):
        return default
    if not nxt.startswith("/"):
        return default
    return nxt
