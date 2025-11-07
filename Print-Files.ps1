<#  Print-Files.ps1
    Examples:
      .\Print-Files.ps1 -Path "C:\invoices\invoice.pdf"
      .\Print-Files.ps1 -Path "C:\invoices\*.pdf" -Printer "HP LaserJet"
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory, ValueFromPipeline, ValueFromPipelineByPropertyName)]
  [string[]]$Path,

  # Optional: print to a specific printer
  [string]$Printer,

  # Give the spooler a moment after each launch
  [int]$DelaySeconds = 3
)

function Write-Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err ($m){ Write-Host "[ERR ] $m" -ForegroundColor Red }

# Ensure Print Spooler is running
try {
  $spooler = Get-Service -Name Spooler -ErrorAction Stop
  if ($spooler.Status -ne 'Running') {
    Write-Info "Starting Print Spooler..."
    Start-Service Spooler
    $spooler.WaitForStatus('Running','00:00:10') | Out-Null
  }
} catch {
  Write-Err "Cannot access Print Spooler: $($_.Exception.Message)"
  throw
}

# Validate printer (if provided)
if ($Printer) {
  $printerObj = $null
  try { $printerObj = Get-Printer -Name $Printer -ErrorAction Stop } catch {}
  if (-not $printerObj) {
    try { $printerObj = Get-CimInstance Win32_Printer -Filter "Name='$Printer'" -ErrorAction Stop } catch {}
  }
  if (-not $printerObj) {
    Write-Err "Printer not found: '$Printer'"
    throw
  }
}

# Try to find Adobe Reader for PDF fallback (optional)
$AcroExe = $null
try {
  $reg = 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\AcroRd32.exe'
  $AcroExe = (Get-ItemProperty -Path $reg -ErrorAction SilentlyContinue).'(default)'
  if (-not $AcroExe) {
    $cand = Get-ChildItem -Path "$env:ProgramFiles\Adobe","${env:ProgramFiles(x86)}\Adobe" `
             -Filter AcroRd32.exe -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($cand) { $AcroExe = $cand.FullName }
  }
  if ($AcroExe) { Write-Info "Adobe Reader: $AcroExe" }
} catch {}

# Expand input paths and print
$allFiles = @()
foreach ($p in $Path) {
  # Wildcards first
  $gci = Get-ChildItem -Path $p -File -ErrorAction SilentlyContinue
  if ($gci) { $allFiles += $gci; continue }

  # Then literal path
  $gi = Get-Item -LiteralPath $p -ErrorAction SilentlyContinue
  if ($gi) { $allFiles += $gi; continue }

  Write-Warn "No files matched: $p"
}

foreach ($f in $allFiles) {
  if (-not (Test-Path -LiteralPath $f.FullName)) { Write-Warn "Missing: $($f.FullName)"; continue }

  Write-Info "Printing: $($f.FullName)"
  $printed = $false

  # Primary: Shell verbs
  try {
    if ($Printer) {
      Start-Process -FilePath $f.FullName -Verb PrintTo -ArgumentList ('"{0}"' -f $Printer) -ErrorAction Stop | Out-Null
    } else {
      Start-Process -FilePath $f.FullName -Verb Print -ErrorAction Stop | Out-Null
    }
    Start-Sleep -Seconds $DelaySeconds
    $printed = $true
  } catch {
    Write-Warn "Shell print failed: $($_.Exception.Message)"
  }

  # Fallback: Adobe Reader for PDFs (silent)
  $ext = ($f.Extension).ToLower()
  if (-not $printed -and $ext -eq '.pdf' -and $AcroExe) {
    try {
      if ($Printer) {
        & $AcroExe /t "`"$($f.FullName)`"" "`"$Printer`""
      } else {
        & $AcroExe /p /h "`"$($f.FullName)`""
      }
      Start-Sleep -Seconds $DelaySeconds
      $printed = $true
    } catch {
      Write-Warn "Adobe fallback failed: $($_.Exception.Message)"
    }
  }

  if ($printed) {
    Write-Host "[DONE] $($f.Name)" -ForegroundColor Green
  } else {
    Write-Err "Failed to print: $($f.FullName)"
  }
}

Write-Info "Finished."
