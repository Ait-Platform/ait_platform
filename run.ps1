#run.ps1
Set-Location "D:/Users/yeshk/Documents/ait_platform"
.\.venv\Scripts\Activate.ps1

# free port 8000
Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue |
  ForEach-Object { Get-Process -Id $_.OwningProcess } |
  Stop-Process -Force

$env:FLASK_APP = "wsgi:app"
$env:FLASK_ENV = "production"

# *** FORCE USING CORRECT VENV ***
D:/Users/yeshk/Documents/ait_platform/.venv/Scripts/flask.exe run --host=0.0.0.0 --port=8000
