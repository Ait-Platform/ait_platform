# app/auth/session_utils.py
def set_identity(session, *, uid, name, email, role, subject="", is_admin=None, keep=False):
    """
    Set canonical session keys once. If keep=True, don’t clear existing session.
    """
    if not keep:
        session.clear()
    email = (email or "").lower()
    role  = "admin" if (is_admin or role == "admin") else role

    session["user_id"]    = uid
    session["user_name"]  = name or (email.split("@")[0].title() if email else "User")
    session["user_email"] = email
    session["email"]      = email              # compatibility
    session["role"]       = role
    session["user_role"]  = role               # compatibility
    session["subject"]    = (subject or "").lower()
    session["is_admin"]   = bool(is_admin or role == "admin")
    session.permanent     = True

