<#
.SYNOPSIS
    Keep `futrisk data record` running until a Beijing-time deadline, restarting it after any crash.

.DESCRIPTION
    The recorder gates itself on the trading calendar (it idles outside sessions) and
    takes a single-instance lock, so restarting it at any time is safe. Every (re)start
    and exit is logged with a timestamp to the console and to <home>\logs\record_forever.log.
    Quick consecutive failures back off 5 s, 10 s, 20 s ... up to 60 s; a run that lasted
    more than 5 minutes resets the back-off.

    Exit codes of the recorder (see src/futrisk/cli/data_cmd.py):
      0   finished (--until reached)   -> stop
      130 stopped by Ctrl-C/Ctrl-Break -> stop
      3   another recorder holds lock  -> stop
      2   bad command-line arguments   -> stop (fix them)
      4   final flush failed           -> restart (and look at the log!)
      1   crash                        -> restart

    Run it from Windows Terminal (in the legacy console, selecting text pauses output).
    Press Ctrl-C to stop both the recorder (it flushes first) and this loop.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\record_forever.ps1

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\record_forever.ps1 -Until "2026-09-30 15:30" -FutriskHome D:\futrisk-data

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\record_forever.ps1 -RecordArgs "--poll-ms 1000"
#>
[CmdletBinding()]
param(
    # Beijing time after which the recorder is not restarted (it also stops itself there).
    [string]$Until = "2026-09-30 15:30",
    # Runtime home (default: <repo>\var). Do not name this $Home: that is a PowerShell automatic variable.
    [string]$FutriskHome = "",
    [int]$BackoffSeconds = 5,
    [int]$MaxBackoffSeconds = 60,
    # Extra arguments for `data record` as ONE string, e.g. -RecordArgs "--poll-ms 1000 --symbols rb2701,IF2612"
    # (a string, not an array: `powershell -File` would otherwise pass "a,b" as a single argument).
    [string]$RecordArgs = ""
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { throw "Python venv not found: $python" }
if (-not $FutriskHome) { $FutriskHome = Join-Path $repo "var" }
$logDir = Join-Path $FutriskHome "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logFile = Join-Path $logDir "record_forever.log"

$beijing = [System.TimeZoneInfo]::FindSystemTimeZoneById("China Standard Time")
$deadline = [datetime]::ParseExact($Until, "yyyy-MM-dd HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)

function Get-BeijingNow {
    return [System.TimeZoneInfo]::ConvertTimeFromUtc([datetime]::UtcNow, $beijing)
}

function Write-Log([string]$Message) {
    $line = "{0:yyyy-MM-dd HH:mm:ss} local | {1:yyyy-MM-dd HH:mm:ss} Beijing | {2}" -f (Get-Date), (Get-BeijingNow), $Message
    Write-Host $line
    Add-Content -Path $logFile -Value $line -Encoding UTF8
}

$extraArgs = @($RecordArgs -split '\s+' | Where-Object { $_ })

$env:PYTHONUTF8 = "1"
Set-Location $repo
$backoff = $BackoffSeconds
$run = 0
Write-Log "record_forever started: home=$FutriskHome until='$Until' (Beijing) extra args: $($extraArgs -join ' ')"

while ((Get-BeijingNow) -lt $deadline) {
    $run++
    Write-Log "start #$run"
    $started = Get-Date
    & $python -m futrisk.main data record --home $FutriskHome --until $Until @extraArgs
    $code = $LASTEXITCODE
    $ranFor = [int]((Get-Date) - $started).TotalSeconds

    if ($code -eq 0) { Write-Log "run #$run finished normally after ${ranFor}s (exit 0)"; break }
    if ($code -eq 130) { Write-Log "run #$run stopped by the user after ${ranFor}s (exit 130)"; break }
    if ($code -eq 3) { Write-Log "run #$run refused: another recorder holds the lock (exit 3)"; break }
    if ($code -eq 2) { Write-Log "run #$run rejected its arguments (exit 2); fix them and start again"; break }
    if ((Get-BeijingNow) -ge $deadline) { Write-Log "run #$run exited with $code after ${ranFor}s; deadline reached"; break }

    if ($ranFor -gt 300) { $backoff = $BackoffSeconds }
    Write-Log "run #$run exited with code $code after ${ranFor}s; restarting in ${backoff}s"
    Start-Sleep -Seconds $backoff
    $backoff = [Math]::Min($backoff * 2, $MaxBackoffSeconds)
}

Write-Log "record_forever finished"
