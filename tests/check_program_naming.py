"""Database-free path/blueprint regression; never invokes create_app."""
import ast
import json
import sys
import types
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
package = types.ModuleType('app')
package.__path__ = [str(ROOT / 'app')]
sys.modules['app'] = package

from flask import Flask
from jinja2 import meta
from sqlalchemy.engine import Engine

with patch.object(Engine, 'connect', side_effect=AssertionError('Database connection forbidden')):
    from app.program_uip import uip_bp
    from app.program_retire import retire_bp

    app = Flask('program_naming_check', template_folder=str(ROOT / 'templates'))
    # Execute only the actual factory's two imports and blueprint registrations.
    tree = ast.parse((ROOT / 'app/__init__.py').read_text(encoding='utf-8-sig'))
    factory = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'create_app')
    selected = []
    for node in factory.body:
        if isinstance(node, ast.ImportFrom) and node.module in ('app.program_uip', 'app.program_retire'):
            selected.append(node)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if (isinstance(call.func, ast.Attribute) and call.func.attr == 'register_blueprint'
                    and call.args and isinstance(call.args[0], ast.Name)
                    and call.args[0].id in ('uip_bp', 'retire_bp')):
                selected.append(node)
    assert len(selected) == 4, 'Both imports and registrations must be present'
    exec(compile(ast.Module(body=selected, type_ignores=[]), '<isolated registrations>', 'exec'), {'app': app})
    rows = sorted([(r.rule, r.endpoint, sorted(r.methods)) for r in app.url_map.iter_rules()
                   if r.endpoint.startswith(('uip_bp.', 'retire_bp.'))])
    baseline = ROOT / 'artifacts/program-path-refactor/routes-before.json'
    if baseline.exists():
        assert json.loads(json.dumps(rows)) == json.loads(baseline.read_text()), 'Route map changed'
    for rule, endpoint, methods in rows:
        assert rule.startswith('/uip' if endpoint.startswith('uip_bp.') else '/retire')
    print('PASS blueprint registration and unchanged route map:', len(rows), 'routes')
    print('UIP:', sum(r[1].startswith('uip_bp.') for r in rows), 'Retirement:', sum(r[1].startswith('retire_bp.') for r in rows))

    count = 0
    for prefix in ('program_uip', 'program_retire'):
        for path in (ROOT / 'templates' / prefix).rglob('*.html'):
            source = path.read_text(encoding='utf-8-sig')
            parsed = app.jinja_env.parse(source)
            for dependency in meta.find_referenced_templates(parsed):
                if dependency is not None:
                    app.jinja_env.loader.get_source(app.jinja_env, dependency)
            count += 1
        for path in (ROOT / 'app' / prefix).rglob('*.py'):
            source = path.read_text(encoding='utf-8-sig')
            parsed = ast.parse(source)
            for node in ast.walk(parsed):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    value = node.value
                    if value.startswith(prefix + '/') and value.endswith('.html'):
                        app.jinja_env.loader.get_source(app.jinja_env, value)
    print('PASS template syntax, inheritance/includes and static render targets:', count, 'templates')
