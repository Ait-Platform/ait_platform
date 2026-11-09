#!/usr/bin/env bash
# Render Native Python build. Gunicorn's start command stays unchanged.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Set this same variable in Render's runtime environment, too.
export PLAYWRIGHT_BROWSERS_PATH=0
python -m pip install -r requirements.txt

# Render Native may restrict apt/sudo. Use its provided libraries in that case;
# the launch/PDF check below must still pass or the entire build fails.
if [[ "$(uname -s)" == "Linux" ]]; then
    if [[ "$(id -u)" == "0" ]]; then
        python -m playwright install-deps chromium
    elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
        sudo -n "$(command -v python)" -m playwright install-deps chromium
    else
        printf '%s\n' 'OS package installation is restricted; verifying Render-provided Linux libraries during the browser launch check.'
    fi
fi

# Run the CLI from the installed, pinned Python package; include headless shell.
python -m playwright install chromium
python scripts/verify_metsoa_pdf_browser.py
