import re

with open('GEMINI.md', 'r', encoding='utf-8') as f:
    content = f.read()

old_db_section = """## 2. Databases (Postgres Strict)
- There is NO SQLite fallback in this project.
- The platform strictly requires PostgreSQL.
- The connection is managed via the DATABASE_URL environment variable.
  - Local Dev: Points to the local PostgreSQL database.
  - Render: Automatically injected by Render to point to their managed Postgres instance."""

new_db_section = """## 2. Databases (Postgres Strict)
- There is NO SQLite fallback in this project.
- The platform strictly requires PostgreSQL.
- **Two Database Environment**: The project uses two distinct databases:
  - Remote (Render): it_platform_db (Live tests and real users).
  - Local (Desktop): it_local_db (Where AI agent test scripts execute).
  - *CRITICAL RULE:* AI scripts run locally. Any configuration rows (like new auth_subject rows) inserted by the AI will ONLY exist locally. You MUST provide the raw psql statements to the Admin so they can manually execute them on the Render database!
- The connection is managed via the DATABASE_URL environment variable."""

content = content.replace(old_db_section, new_db_section)

with open('GEMINI.md', 'w', encoding='utf-8') as f:
    f.write(content)
