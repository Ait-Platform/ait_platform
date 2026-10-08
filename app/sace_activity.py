"""Shared SACE routing. Adapters retain ownership of authority and continuation rules.

Resolution never provisions authority. Competing continuations are not ordered by
registration: only a unique continuation determines an activity without context.
"""
from typing import Protocol
from urllib.parse import urljoin, urlsplit
from flask import current_app, redirect, render_template, request
from flask_login import current_user


class ActivityAdapter(Protocol):
    identifier: str
    display_name: str

    def entry(self, target=None, *, selected=False): ...
    def recognizes(self, path, subject): ...
    def continuation(self, target=None, *, selected=False): ...
    def has_authority(self) -> bool: ...


def registry() -> dict[str, ActivityAdapter]:
    from app.program_sace.activity import ReadingActivity
    from app.program_sace_home.activity import HomeActivity
    return current_app.config.get('SACE_ACTIVITY_REGISTRY', {
        'sace_endorsement': ReadingActivity(),
        'sace_home_endorsement': HomeActivity(),
    })


def activities():
    return tuple(registry().values())


def subject_slugs():
    # Subject associations are registry metadata, never evidence of authority.
    return frozenset(registry())


def local_path(target):
    if not target:
        return None
    parsed = urlsplit(urljoin(request.host_url, target))
    origin = urlsplit(request.host_url)
    if parsed.scheme not in ('http', 'https') or parsed.netloc != origin.netloc:
        return None
    return parsed.path


def explicit_activity(target=None, subject=None):
    matches = [a for a in activities() if a.recognizes(local_path(target), subject)]
    return matches[0] if len(matches) == 1 else None


def choose(authorised):
    from app.program_sace import access as reading_access
    from app.program_sace_home import service as home_service
    identifiers = {a.identifier for a in authorised}
    controller = (('reading' in identifiers and reading_access.is_controller())
        or ('home' in identifiers and bool(home_service.controller())))
    response = current_app.make_response(render_template('sace/choose_activity.html',
        controller=controller, activities=[dict(identifier=a.identifier, name=a.display_name, destination=a.entry())
                    for a in authorised]))
    response.headers['Cache-Control'] = 'private, no-store'
    return response


def resolve_response(target=None, subject=None):
    if not current_user.is_authenticated:
        return None
    adapters = tuple(activities())
    explicit = explicit_activity(target, subject)
    if explicit:
        # A subject origin cannot make an external or unrelated next URL trusted.
        if not explicit.recognizes(local_path(target), None):
            target = None
        destination = explicit.continuation(target, selected=True)
        if destination:
            return redirect(destination)
        # Preserve detailed guarded URLs, including their lifecycle/evidence 409s.
        entry = explicit.entry(target, selected=True)
        return redirect(entry) if entry else None
    conflicting_context = any(a.recognizes(local_path(target), subject) for a in adapters)
    if not conflicting_context and target and local_path(target) not in ('/dashboard', '/bridge'):
        return None
    # Platform identity wins only at a context-free platform entry. Explicit
    # activity entry above retains its own authority and continuation guards.
    from app.auth.routes import check_admin
    if not conflicting_context and check_admin(current_user.email):
        return None
    continuations = [] if conflicting_context else [(a, a.continuation(None)) for a in adapters]
    valid = [(a, destination) for a, destination in continuations if destination]
    if len(valid) == 1:
        adapter, _ = valid[0]
        destination = adapter.continuation(None, selected=True)
        if destination:
            return redirect(destination)
    authorised = [a for a in adapters if a.has_authority()]
    if len(authorised) > 1:
        return choose(authorised)
    if len(authorised) == 1:
        return redirect(authorised[0].entry(selected=True))
    return None
