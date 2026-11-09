# Render Native Python: METSOA Chromium dependency

Apply these settings manually to the existing AIT web service:

- Build Command: `bash scripts/render_build.sh`
- Environment variable (build and runtime): `PLAYWRIGHT_BROWSERS_PATH=0`
- Keep the existing Start Command exactly as configured:

```text
gunicorn "app:create_app()" --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120 --access-logfile - --error-logfile -
```

The build installs the existing pinned Python requirements, then installs the
matching Chromium and headless shell inside the Playwright package. Keeping the
same browser-path setting at runtime makes that bundled installation available
to METSOA after each deployment.

Linux dependencies are installed through Playwright when the build permits root
or passwordless sudo. On restricted Render Native builders, the service uses
Render's provided libraries. The final verification launches Chromium and creates
an in-memory A4 PDF; missing libraries, missing browsers or invalid PDF output
fail the build. No errors are ignored and no application/database startup occurs.

After applying the settings, deploy and confirm the build log contains JSON with
`browser_path: "0"`, the Playwright/Chromium versions and a nonzero `pdf_bytes`.
Then verify METSOA through Utilities Output. Dashboard settings and deployment
are not changed automatically by this repository update.
