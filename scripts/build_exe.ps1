<#
.SYNOPSIS
    Build the futrisk one-folder Windows executable, smoke-test it and package it (01 section 7.1 D1).

.DESCRIPTION
    1. Dashboard: npm ci + npm run build in web\ (writes src\futrisk\web_static\dist).
       -SkipWeb reuses the existing dist (a warning is printed when web\ sources are newer).
    2. PyInstaller: packaging\futrisk.spec -> dist\futrisk\ (clean build).
    3. Smoke tests of dist\futrisk\futrisk.exe. Every test uses its own temporary home whose
       config\ copies the bundled defaults with free loopback ports, so a futrisk that already
       runs on the default ports (7xxx, 8000) is neither disturbed nor mistaken for ours:
         version; config validate; doctor --json (frozen-specific checks must PASS);
         svc api-gateway (GET /api/v1/health and the dashboard at /, then a clean stop);
         backtest run breaker_synthetic (process pool; the manifest records the exe's code hash);
         up for -UpSeconds (every service RUNNING in `ps`, `demo --headless` seeds the books,
         `status` healthy), then down;
         the dist folder copied under a path with Chinese characters (%TEMP%\<Chinese>\futrisk).
    4. dist\futrisk-win64-<version>.zip and dist\futrisk-win64-<version>.zip.sha256.

    Exits with code 1 and a message naming the failed step. No zip is written when a smoke test
    fails. Only processes started by this script are ever stopped.

    Compatible with Windows PowerShell 5.1 (the file is plain ASCII on purpose).

.PARAMETER SkipWeb
    Do not run npm; use the existing src\futrisk\web_static\dist.
.PARAMETER SkipTests
    Build (and zip) without smoke tests.
.PARAMETER SkipUp
    Skip the `futrisk up` system test (the other smoke tests still run).
.PARAMETER UpSeconds
    How long the system test keeps the system up (default 60).
.PARAMETER Python
    Python used to run PyInstaller (default: .venv\Scripts\python.exe of the repository).
.PARAMETER KeepTemp
    Keep the temporary homes and logs (they are always kept when a test fails).

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_exe.ps1
.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_exe.ps1 -SkipWeb -UpSeconds 90
#>
[CmdletBinding()]
param(
    [switch]$SkipWeb,
    [switch]$SkipTests,
    [switch]$SkipUp,
    [int]$UpSeconds = 60,
    [string]$Python = "",
    [switch]$KeepTemp
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$Root = Split-Path -Parent $PSScriptRoot
$DistRoot = Join-Path $Root 'dist'
$DistDir = Join-Path $DistRoot 'futrisk'
$Exe = Join-Path $DistDir 'futrisk.exe'
$Spec = Join-Path $Root 'packaging\futrisk.spec'
$WebDir = Join-Path $Root 'web'
$WebIndex = Join-Path $Root 'src\futrisk\web_static\dist\index.html'
if (-not $Python) { $Python = Join-Path $Root '.venv\Scripts\python.exe' }

$Stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$TempRoot = Join-Path ([IO.Path]::GetTempPath()) "futrisk-build-$Stamp"
# A Chinese directory name ("futures risk test", U+671F U+8D27 U+98CE U+63A7 U+6D4B U+8BD5) built
# from code points: the script stays ASCII, which Windows PowerShell 5.1 reads correctly whatever
# the system code page.
$CnName = -join ([char[]](0x671F, 0x8D27, 0x98CE, 0x63A7, 0x6D4B, 0x8BD5))
$CnRoot = Join-Path ([IO.Path]::GetTempPath()) $CnName

$Results = New-Object System.Collections.ArrayList
$Timings = [ordered]@{}
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

# ============================================================================ helpers
function Write-Step([string]$Message) {
    Write-Host ''
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Stop-Build([string]$Message) {
    throw [System.Exception]::new($Message)
}

function Add-Result([string]$Name, [bool]$Ok, [string]$Detail) {
    [void]$Results.Add([pscustomobject]@{ Step = $Name; Result = $(if ($Ok) { 'PASS' } else { 'FAIL' }); Detail = $Detail })
    if ($Ok) { Write-Host "  PASS  $Name  $Detail" -ForegroundColor Green }
    else { Write-Host "  FAIL  $Name  $Detail" -ForegroundColor Red }
}

function Get-Prop($Object, [string]$Name) {
    # property of a ConvertFrom-Json object, $null when absent (JSON omits default values;
    # a plain $obj.name throws under Set-StrictMode)
    if ($null -eq $Object) { return $null }
    $p = $Object.PSObject.Properties[$Name]
    if ($null -eq $p) { return $null }
    return $p.Value
}

function Limit-Text([string]$Text, [int]$Max = 400) {
    if ($null -eq $Text) { return '' }
    $t = ($Text -replace '\s+', ' ').Trim()
    if ($t.Length -gt $Max) { return $t.Substring(0, $Max) + '...' }
    return $t
}

function ConvertTo-ArgString([string]$Arg) {
    # one argument quoted for CommandLineToArgvW / the MSVC runtime
    if ($Arg -eq '') { return '""' }
    if ($Arg -notmatch '[\s"]') { return $Arg }
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.Append('"')
    $slashes = 0
    foreach ($ch in $Arg.ToCharArray()) {
        if ($ch -eq [char]'\') { $slashes++; continue }
        if ($ch -eq [char]'"') {
            [void]$sb.Append([string]::new([char]'\', 2 * $slashes + 1)).Append('"')
            $slashes = 0
            continue
        }
        if ($slashes -gt 0) { [void]$sb.Append([string]::new([char]'\', $slashes)); $slashes = 0 }
        [void]$sb.Append($ch)
    }
    if ($slashes -gt 0) { [void]$sb.Append([string]::new([char]'\', 2 * $slashes)) }
    [void]$sb.Append('"')
    return $sb.ToString()
}

function Join-Args([string[]]$Arguments) {
    return (($Arguments | ForEach-Object { ConvertTo-ArgString $_ }) -join ' ')
}

function Invoke-Loud([string]$FilePath, [string[]]$Arguments) {
    # Run with the output shown; stderr lines are plain output here, not terminating errors
    # (Windows PowerShell turns redirected native stderr into error records).
    $saved = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $FilePath @Arguments 2>&1 | ForEach-Object { Write-Host "  $_" }
        return $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $saved
    }
}

function Invoke-Native {
    # Run to completion; stdout/stderr captured as UTF-8 (futrisk writes UTF-8 whatever the console code page).
    param([string]$FilePath, [string[]]$Arguments = @(), [int]$TimeoutSec = 180, [string]$WorkDir = '')
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $FilePath
    $psi.Arguments = Join-Args $Arguments
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.StandardOutputEncoding = $Utf8NoBom
    $psi.StandardErrorEncoding = $Utf8NoBom
    if ($WorkDir) { $psi.WorkingDirectory = $WorkDir }
    $p = [System.Diagnostics.Process]::Start($psi)
    $outTask = $p.StandardOutput.ReadToEndAsync()
    $errTask = $p.StandardError.ReadToEndAsync()
    $finished = $p.WaitForExit($TimeoutSec * 1000)
    if (-not $finished) {
        try { $p.Kill() } catch { }
        [void]$p.WaitForExit(10000)
        return [pscustomobject]@{ Code = -1; Out = ''; Err = "timed out after $TimeoutSec s"; TimedOut = $true }
    }
    $p.WaitForExit()  # drains the asynchronous readers
    return [pscustomobject]@{ Code = $p.ExitCode; Out = $outTask.Result; Err = $errTask.Result; TimedOut = $false }
}

function Start-Background([string]$FilePath, [string[]]$Arguments, [string]$LogBase) {
    # Long-running process with stdout/stderr in files; returns the Process.
    $p = Start-Process -FilePath $FilePath -ArgumentList (Join-Args $Arguments) -NoNewWindow -PassThru `
        -RedirectStandardOutput "$LogBase.out.log" -RedirectStandardError "$LogBase.err.log"
    $null = $p.Handle  # keeps ExitCode available after the process ends (Windows PowerShell quirk)
    return $p
}

function Get-FreePorts([int]$Count) {
    $listeners = @()
    $ports = @()
    try {
        for ($i = 0; $i -lt $Count; $i++) {
            $l = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
            $l.Start()
            $listeners += $l
            $ports += $l.LocalEndpoint.Port
        }
    } finally {
        foreach ($l in $listeners) { $l.Stop() }
    }
    return $ports
}

function Get-Http([string]$Url, [int]$TimeoutMs = 3000) {
    # $null when nothing answers; the status code and body otherwise (no proxy: loopback only).
    try {
        $req = [System.Net.HttpWebRequest]::Create($Url)
        $req.Proxy = $null
        $req.Timeout = $TimeoutMs
        $req.ReadWriteTimeout = $TimeoutMs
        try {
            $resp = $req.GetResponse()
        } catch [System.Net.WebException] {
            $resp = $_.Exception.Response
            if ($null -eq $resp) { return $null }
        }
        try {
            $reader = New-Object System.IO.StreamReader($resp.GetResponseStream(), [System.Text.Encoding]::UTF8)
            $body = $reader.ReadToEnd()
            return [pscustomobject]@{ Status = [int]$resp.StatusCode; Body = $body; ContentType = [string]$resp.ContentType }
        } finally {
            $resp.Close()
        }
    } catch {
        return $null
    }
}

function New-SmokeHome([string]$HomeDir, [string]$ConfigSource) {
    # <home>\config = the bundled defaults with every loopback port replaced by a free one.
    # ensure_home_config() keeps an existing <home>\config\app.yaml and snapshots it as version 1,
    # so the services of this home run on these ports.
    if (Test-Path -LiteralPath $HomeDir) { Remove-Item -LiteralPath $HomeDir -Recurse -Force }
    $cfgDir = Join-Path $HomeDir 'config'
    New-Item -ItemType Directory -Force -Path $cfgDir | Out-Null
    Copy-Item -Path (Join-Path $ConfigSource '*') -Destination $cfgDir -Recurse -Force
    $files = @('services.yaml', 'sim.yaml', 'feeds.yaml') | ForEach-Object { Join-Path $cfgDir $_ }
    $rx = New-Object System.Text.RegularExpressions.Regex('(?<=127\.0\.0\.1:)\d{2,5}(?!\d)|(?<=\bport:\s*)\d{2,5}(?!\d)')
    $old = New-Object System.Collections.Generic.List[string]
    foreach ($f in $files) {
        foreach ($m in $rx.Matches([IO.File]::ReadAllText($f, [Text.Encoding]::UTF8))) {
            if (-not $old.Contains($m.Value)) { $old.Add($m.Value) }
        }
    }
    if ($old.Count -lt 10) { Stop-Build "unexpected config layout in $ConfigSource (found $($old.Count) ports)" }
    $new = @(Get-FreePorts $old.Count)
    $map = @{}
    for ($i = 0; $i -lt $old.Count; $i++) { $map[$old[$i]] = [string]$new[$i] }
    $evaluator = [System.Text.RegularExpressions.MatchEvaluator] { param($m) $map[$m.Value] }.GetNewClosure()
    foreach ($f in $files) {
        $text = $rx.Replace([IO.File]::ReadAllText($f, [Text.Encoding]::UTF8), $evaluator)
        [IO.File]::WriteAllText($f, $text, $Utf8NoBom)
    }
    $svc = [IO.File]::ReadAllText($files[0], [Text.Encoding]::UTF8)
    $m = [regex]::Match($svc, 'http:\s*"(http://127\.0\.0\.1:\d+)"')
    if (-not $m.Success) { Stop-Build "no api-gateway http endpoint in $($files[0])" }
    # "<parent dir name>\<home dir name>": found in the command line of every process of this home,
    # whether the temp path is spelled long or 8.3-short (the services resolve --home)
    $marker = (Split-Path -Leaf (Split-Path -Parent $HomeDir)) + '\' + (Split-Path -Leaf $HomeDir)
    return [pscustomobject]@{ Home = $HomeDir; Api = $m.Groups[1].Value; Ports = $map; Marker = $marker }
}

function Get-OurProcesses([string]$Marker) {
    # futrisk processes whose command line contains $Marker (a temp directory name of this build)
    $all = Get-CimInstance Win32_Process -Filter "Name = 'futrisk.exe'"
    return @($all | Where-Object { $_.CommandLine -and $_.CommandLine.Contains($Marker) })
}

function Stop-OurProcesses([string]$Marker) {
    $left = @(Get-OurProcesses $Marker)
    foreach ($proc in $left) {
        Write-Host "  stopping leftover pid $($proc.ProcessId): $(Limit-Text $proc.CommandLine 160)" -ForegroundColor Yellow
        & taskkill.exe /PID $proc.ProcessId /T /F 2>&1 | Out-Null
    }
    return $left.Count
}

function Test-ApiGateway([string]$ExePath, [string]$HomeDir, [string]$ConfigSource, [string]$Label) {
    # Start `svc api-gateway` in its own home; check /api/v1/health and the dashboard at /; stop it by
    # ending its --parent-pid (a sleeper we own): the service then shuts down gracefully (exit 0).
    $h = New-SmokeHome $HomeDir $ConfigSource
    $sleeper = Start-Process -FilePath 'powershell.exe' -ArgumentList '-NoProfile -NonInteractive -Command Start-Sleep -Seconds 900' -WindowStyle Hidden -PassThru
    $gw = Start-Background $ExePath @('svc', 'api-gateway', '--home', $h.Home, '--parent-pid', [string]$sleeper.Id) (Join-Path $TempRoot "$Label-api-gateway")
    try {
        $health = $null
        $deadline = (Get-Date).AddSeconds(60)
        while ((Get-Date) -lt $deadline -and -not $gw.HasExited) {
            $health = Get-Http "$($h.Api)/api/v1/health"
            if ($null -ne $health -and $health.Status -eq 200) { break }
            Start-Sleep -Milliseconds 500
        }
        if ($null -eq $health -or $health.Status -ne 200) {
            $why = $(if ($gw.HasExited) { "process exited with code $($gw.ExitCode)" } else { 'no answer within 60 s' })
            Add-Result "$Label api-gateway health" $false "$($h.Api)/api/v1/health: $why; see $TempRoot\$Label-api-gateway.err.log"
        } else {
            $state = ''
            try { $state = Get-Prop (Get-Prop ($health.Body | ConvertFrom-Json) 'data') 'state' } catch { }
            Add-Result "$Label api-gateway health" $true "$($h.Api)/api/v1/health 200 (service state $state)"
            # index.html and the first script it loads (the bundled assets, not just the page)
            $page = Get-Http "$($h.Api)/" 5000
            $isDashboard = ($null -ne $page -and $page.Status -eq 200 -and $page.Body -match '<div id="app"' -and $page.ContentType -match 'text/html')
            $detail = $(if ($null -eq $page) { 'no answer' } else { "HTTP $($page.Status) $($page.ContentType), $($page.Body.Length) bytes" })
            if ($isDashboard) {
                $asset = [regex]::Match($page.Body, 'src="\./(assets/[^"]+\.js)"')
                $js = $(if ($asset.Success) { Get-Http "$($h.Api)/$($asset.Groups[1].Value)" 5000 } else { $null })
                $isDashboard = ($null -ne $js -and $js.Status -eq 200)
                $detail += $(if ($asset.Success) { "; $($asset.Groups[1].Value) " + $(if ($null -ne $js) { "HTTP $($js.Status)" } else { 'no answer' }) } else { '; no script tag' })
            }
            Add-Result "$Label dashboard /" $isDashboard $detail
        }
    } finally {
        Stop-Process -Id $sleeper.Id -Force -ErrorAction SilentlyContinue
        $stopped = $gw.WaitForExit(30000)
        if (-not $stopped) { Stop-OurProcesses $h.Marker | Out-Null }
    }
    $clean = $stopped -and $gw.ExitCode -eq 0
    Add-Result "$Label api-gateway stop" $clean $(if ($stopped) { "exit code $($gw.ExitCode) after its parent ended" } else { 'did not stop within 30 s (killed)' })
}

function Test-Doctor([string]$ExePath, [string]$HomeDir, [string]$Label) {
    $r = Invoke-Native $ExePath @('doctor', '--json', '--home', $HomeDir) 120
    try {
        $doc = $r.Out | ConvertFrom-Json
    } catch {
        Add-Result "$Label doctor --json" $false "exit $($r.Code), unparsable output: $(Limit-Text ($r.Out + ' ' + $r.Err))"
        return
    }
    # the checks a broken freeze would fail; ports / clock sync depend on the machine and are only reported
    $must = @{ 'build' = 'INFO'; 'tz Asia/Shanghai' = 'PASS'; 'dashboard' = 'PASS'; 'pyzmq' = 'PASS'; 'sqlite' = 'PASS'; 'non-ASCII path' = 'PASS' }
    $checks = @(Get-Prop $doc 'checks')
    $bad = @()
    foreach ($name in $must.Keys) {
        $c = @($checks | Where-Object { (Get-Prop $_ 'check') -eq $name })
        if ($c.Count -eq 0) { $bad += "$name missing"; continue }
        $status = Get-Prop $c[0] 'status'
        $detail = [string](Get-Prop $c[0] 'detail')
        if ($status -ne $must[$name]) { $bad += "$name ${status}: $(Limit-Text $detail 160)" }
        if ($name -eq 'build' -and $detail -notmatch 'frozen') { $bad += "build is not frozen: $detail" }
    }
    $failed = @($checks | Where-Object { (Get-Prop $_ 'status') -eq 'FAIL' } | ForEach-Object { Get-Prop $_ 'check' })
    $note = $(if ($failed.Count) { "; other FAIL checks (machine state, not the build): $($failed -join ', ')" } else { '' })
    if ($bad.Count) { Add-Result "$Label doctor --json" $false (($bad -join '; ') + $note) }
    else { Add-Result "$Label doctor --json" $true ("frozen exe, tz/dashboard/pyzmq/sqlite/non-ASCII PASS" + $note) }
}

function Test-Backtest([string]$ExePath, [string]$OutDir) {
    # layer D synthetic scenario: the bundled config/backtest, a spawn process pool re-entering the
    # exe (freeze_support), and the reproducibility manifest's code identity of the frozen build
    $r = Invoke-Native $ExePath @('backtest', 'run', 'breaker_synthetic', '--workers', '2', '--out', $OutDir) 300
    $manifest = Join-Path $OutDir 'manifest.json'
    if ($r.Code -ne 0 -or -not (Test-Path -LiteralPath $manifest)) {
        Add-Result 'backtest breaker_synthetic' $false ("exit {0} (2 = scenario checks failed): {1}" -f $r.Code, (Limit-Text ($r.Out + ' ' + $r.Err) 300))
        return
    }
    $text = [IO.File]::ReadAllText($manifest, [Text.Encoding]::UTF8)
    $code = [regex]::Match($text, '"code_hash"\s*:\s*"([0-9a-f]*)"').Groups[1].Value
    $git = [regex]::Match($text, '"git_sha"\s*:\s*"([^"]*)"').Groups[1].Value
    $emptySha256 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'  # hash of no input at all
    $ok = ($code.Length -eq 64 -and $code -ne $emptySha256)
    $how = $(if ($ok) { 'non-empty' } else { 'EMPTY or missing' })
    Add-Result 'backtest breaker_synthetic' $ok ("exit 0, scenario checks passed; manifest code_hash {0} ({1}), git_sha {2}" -f $code, $how, $git)
}

function Get-PsItems([string]$ExePath, [string]$HomeDir) {
    # items of `futrisk ps --json` (svc.ps), or $null when the supervisor does not answer
    $r = Invoke-Native $ExePath @('ps', '--json', '--home', $HomeDir) 30
    if ($r.Code -ne 0) { return $null }
    try { $doc = $r.Out | ConvertFrom-Json } catch { return $null }
    return , @(Get-Prop $doc 'items')  # the comma keeps a one-element array an array
}

function Format-Ps($Items) {
    return (@($Items | ForEach-Object {
        $hb = Get-Prop $_ 'heartbeat_state'
        "$(Get-Prop $_ 'instance')=$(Get-Prop $_ 'state')" + $(if ($hb) { "/$hb" } else { '' })
    }) -join ' ')
}

function Get-NotRunning($Items) {
    return , @($Items | Where-Object { (Get-Prop $_ 'state') -ne 'RUNNING' })
}

function Get-Restarts($Items) {
    $sum = 0
    foreach ($i in @($Items)) { $n = Get-Prop $i 'restarts'; if ($n) { $sum += [int]$n } }
    return $sum
}

function Test-Up([string]$ExePath, [string]$HomeDir, [string]$ConfigSource) {
    $h = New-SmokeHome $HomeDir $ConfigSource
    $started = Get-Date
    $up = Start-Background $ExePath @('up', '--home', $h.Home, '--headless') (Join-Path $TempRoot 'up')
    $allRunningAt = $null
    $last = $null
    try {
        $deadline = $started.AddSeconds([Math]::Max(120, $UpSeconds + 30))
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 3
            if ($up.HasExited) { break }
            $items = Get-PsItems $ExePath $h.Home
            if ($null -eq $items) { continue }
            $last = $items
            $elapsed = ((Get-Date) - $started).TotalSeconds
            Write-Host ("  {0,5:N0}s  {1}" -f $elapsed, (Format-Ps $items))
            if ($items.Count -gt 0 -and (Get-NotRunning $items).Count -eq 0) {
                if ($null -eq $allRunningAt) {
                    $allRunningAt = $elapsed
                    # what `futrisk demo` does next on a running system: wait for the dashboard, then
                    # seed the first-day books through the REST API (risk-engine shards then load them)
                    $demo = Invoke-Native $ExePath @('demo', '--headless', '--home', $h.Home, '--timeout', '90') 150
                    $demoText = Limit-Text (($demo.Out + ' ' + $demo.Err) -replace '\x1b\[[0-9;]*m', '') 300
                    Add-Result 'up: demo --headless (seed books)' ($demo.Code -eq 0) ("exit {0}: {1}" -f $demo.Code, $demoText)
                }
                if ($elapsed -ge $UpSeconds) { break }  # kept up for the requested time, still all RUNNING
            }
        }
        if ($up.HasExited) {
            Add-Result 'up: all services RUNNING' $false "futrisk up exited with code $($up.ExitCode); see $TempRoot\up.err.log and $($h.Home)\logs"
            return
        }
        $finalOk = ($null -ne $last -and $last.Count -gt 0 -and (Get-NotRunning $last).Count -eq 0)
        $restarts = Get-Restarts $last
        $elapsedNow = ((Get-Date) - $started).TotalSeconds
        if ($finalOk) {
            Add-Result 'up: all services RUNNING' $true ("{0} instances RUNNING after {1:N0}s, still RUNNING at {2:N0}s; restarts {3}; process/heartbeat state: {4}" -f $last.Count, $allRunningAt, $elapsedNow, $restarts, (Format-Ps $last))
        } else {
            $state = $(if ($null -ne $last) { Format-Ps $last } else { 'ps never answered' })
            Add-Result 'up: all services RUNNING' $false ("after {0:N0}s: {1}; restarts {2}; logs in {3}\logs" -f $elapsedNow, $state, $restarts, $h.Home)
        }
        $st = Invoke-Native $ExePath @('status', '--home', $h.Home) 60
        Add-Result 'up: status' ($st.Code -eq 0) ("exit {0}: {1}" -f $st.Code, (Limit-Text $st.Out 300))
    } finally {
        $down = Invoke-Native $ExePath @('down', '--home', $h.Home, '--timeout', '120') 180
        $exited = $up.WaitForExit(60000)
        Start-Sleep -Seconds 2  # children exit right after the supervisor
        $leftover = Stop-OurProcesses $h.Marker
        $ok = ($down.Code -eq 0 -and $exited -and $up.ExitCode -eq 0 -and $leftover -eq 0)
        $detail = "down exit $($down.Code); supervisor " + $(if ($exited) { "exit $($up.ExitCode)" } else { 'still running (killed)' }) + "; leftover processes $leftover"
        Add-Result 'up: down' $ok $detail
    }
}

function New-Zip([string]$SourceDir, [string]$ZipPath) {
    # <leaf of SourceDir>/... entries with forward slashes (ZipFile.CreateFromDirectory writes
    # backslashes under Windows PowerShell 5.1); the default home var\ is never shipped.
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $base = (Get-Item -LiteralPath $SourceDir).FullName.TrimEnd('\')
    $prefix = Split-Path -Leaf $base
    $stream = [IO.File]::Open($ZipPath, [IO.FileMode]::CreateNew)
    $archive = New-Object System.IO.Compression.ZipArchive($stream, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        foreach ($file in Get-ChildItem -LiteralPath $base -Recurse -File) {
            $rel = $file.FullName.Substring($base.Length + 1).Replace('\', '/')
            if ($rel -like 'var/*') { continue }
            [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $archive, $file.FullName, "$prefix/$rel", [System.IO.Compression.CompressionLevel]::Optimal)
        }
    } finally {
        $archive.Dispose()
        $stream.Dispose()
    }
}

# ============================================================================ main
$exitCode = 0
$testsFailed = $false
$savedHome = $env:FUTRISK_HOME
try {
    $total = [Diagnostics.Stopwatch]::StartNew()
    Set-Location $Root
    if (-not (Test-Path -LiteralPath $Python)) { Stop-Build "Python not found: $Python (run 'poetry install' first or pass -Python)" }
    $pyiVersion = & $Python -m PyInstaller --version 2>$null
    if ($LASTEXITCODE -ne 0) { Stop-Build "PyInstaller is not installed in $Python (poetry install --with dev)" }
    $initText = [IO.File]::ReadAllText((Join-Path $Root 'src\futrisk\__init__.py'))
    $vm = [regex]::Match($initText, '__version__\s*=\s*"([^"]+)"')
    if (-not $vm.Success) { Stop-Build 'cannot read __version__ from src\futrisk\__init__.py' }
    $Version = $vm.Groups[1].Value
    Write-Host "futrisk $Version, PyInstaller $pyiVersion, $Python"

    # ------------------------------------------------------------------ 1. dashboard
    Write-Step '1/4 dashboard (web/)'
    $sw = [Diagnostics.Stopwatch]::StartNew()
    if ($SkipWeb) {
        if (-not (Test-Path -LiteralPath $WebIndex)) { Stop-Build "$WebIndex is missing: run without -SkipWeb" }
        $distTime = (Get-Item -LiteralPath $WebIndex).LastWriteTime
        $srcItems = @(Get-ChildItem -Path (Join-Path $WebDir 'src') -Recurse -File) + @(Get-Item (Join-Path $WebDir 'index.html'), (Join-Path $WebDir 'package.json'))
        $newest = $srcItems | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($newest.LastWriteTime -gt $distTime) {
            Write-Host "  WARNING: web sources are newer than the built dashboard ($($newest.FullName), $($newest.LastWriteTime) > $distTime)" -ForegroundColor Yellow
        }
        Write-Host "  -SkipWeb: using $WebIndex ($distTime)"
    } else {
        $npm = Get-Command 'npm.cmd' -ErrorAction SilentlyContinue
        if ($null -eq $npm) { Stop-Build 'npm.cmd not found: install Node.js 20 or run with -SkipWeb' }
        Push-Location $WebDir
        try {
            $code = Invoke-Loud $npm.Source @('ci', '--no-audit', '--no-fund')
            if ($code -ne 0) { Stop-Build "npm ci failed (exit $code)" }
            $code = Invoke-Loud $npm.Source @('run', 'build')
            if ($code -ne 0) { Stop-Build "npm run build failed (exit $code): vue-tsc type check or vite build" }
        } finally {
            Pop-Location
        }
        if (-not (Test-Path -LiteralPath $WebIndex)) { Stop-Build "npm run build did not produce $WebIndex" }
    }
    $Timings['dashboard'] = $sw.Elapsed.TotalSeconds

    # ------------------------------------------------------------------ 2. PyInstaller
    Write-Step '2/4 PyInstaller (clean one-folder build)'
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $running = @(Get-CimInstance Win32_Process -Filter "Name = 'futrisk.exe'" | Where-Object { $_.ExecutablePath -and $_.ExecutablePath.StartsWith($DistDir, [StringComparison]::OrdinalIgnoreCase) })
    if ($running.Count) {
        Stop-Build ("futrisk.exe from $DistDir is running (pid " + (($running | ForEach-Object { $_.ProcessId }) -join ', ') + '): stop it first (futrisk down)')
    }
    if (Test-Path -LiteralPath $DistDir) { Remove-Item -LiteralPath $DistDir -Recurse -Force }
    $pyiLog = Join-Path $Root 'build\pyinstaller.log'
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $pyiLog) | Out-Null
    $lines = New-Object System.Collections.Generic.List[string]
    $saved = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        # full log in build\pyinstaller.log; the console gets milestones, warnings and errors
        & $Python -m PyInstaller $Spec --noconfirm --clean --distpath $DistRoot --workpath (Join-Path $Root 'build\pyinstaller') 2>&1 |
            ForEach-Object {
                $line = "$_"
                $lines.Add($line)
                if ($line -match 'WARNING|ERROR|Error|Traceback|Building (PYZ|EXE|COLLECT) |Build complete|Analyzing ') { Write-Host "  $line" }
            }
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $saved
        [IO.File]::WriteAllLines($pyiLog, $lines, $Utf8NoBom)
    }
    if ($code -ne 0) { Stop-Build "PyInstaller failed (exit $code); see $pyiLog" }
    if (-not (Test-Path -LiteralPath $Exe)) { Stop-Build "PyInstaller did not produce $Exe" }
    $Timings['pyinstaller'] = $sw.Elapsed.TotalSeconds
    $sizeMb = ((Get-ChildItem -LiteralPath $DistDir -Recurse -File | Measure-Object -Property Length -Sum).Sum / 1MB)
    Write-Host ("  {0} ({1:N0} MB, {2:N0} s)" -f $DistDir, $sizeMb, $Timings['pyinstaller'])

    # ------------------------------------------------------------------ 3. smoke tests
    if ($SkipTests) {
        Write-Step '3/4 smoke tests skipped (-SkipTests)'
    } else {
        Write-Step "3/4 smoke tests (temporary homes under $TempRoot)"
        $sw = [Diagnostics.Stopwatch]::StartNew()
        New-Item -ItemType Directory -Force -Path $TempRoot | Out-Null
        # a command run without --home must not create dist\futrisk\var
        $env:FUTRISK_HOME = Join-Path $TempRoot 'default-home'
        $bundledConfig = Join-Path $DistDir '_internal\config'

        $r = Invoke-Native $Exe @('version') 60
        Add-Result 'version' ($r.Code -eq 0 -and $r.Out.Trim() -eq "futrisk $Version") ("exit {0}: {1}" -f $r.Code, (Limit-Text ($r.Out + $r.Err)))

        $r = Invoke-Native $Exe @('config', 'validate') 60
        Add-Result 'config validate' ($r.Code -eq 0 -and $r.Out -match '^OK') ("exit {0}: {1}" -f $r.Code, (Limit-Text ($r.Out + $r.Err) 300))

        Test-Doctor $Exe (Join-Path $TempRoot 'doctor-home') 'exe'
        Test-ApiGateway $Exe (Join-Path $TempRoot 'gw-home') $bundledConfig 'exe'
        Test-Backtest $Exe (Join-Path $TempRoot 'bt-breaker-synthetic')

        if ($SkipUp) {
            Write-Host '  up test skipped (-SkipUp)' -ForegroundColor Yellow
        } elseif (-not (Test-Path -LiteralPath (Join-Path $Root 'src\futrisk\services\risk_engine\main.py'))) {
            Write-Host '  up test skipped: src\futrisk\services\risk_engine\main.py does not exist yet' -ForegroundColor Yellow
        } else {
            Write-Host "  up: futrisk up for ~$UpSeconds s"
            Test-Up $Exe (Join-Path $TempRoot 'up-home') $bundledConfig
        }

        # the same build under a path with Chinese characters (home included)
        $cnDist = Join-Path $CnRoot 'futrisk'
        if (Test-Path -LiteralPath $CnRoot) { Remove-Item -LiteralPath $CnRoot -Recurse -Force }
        New-Item -ItemType Directory -Force -Path $CnRoot | Out-Null
        Copy-Item -LiteralPath $DistDir -Destination $CnRoot -Recurse -Force
        $cnExe = Join-Path $cnDist 'futrisk.exe'
        $r = Invoke-Native $cnExe @('version') 60
        Add-Result 'CN path: version' ($r.Code -eq 0 -and $r.Out.Trim() -eq "futrisk $Version") ("{0}: exit {1} {2}" -f $cnExe, $r.Code, (Limit-Text ($r.Out + $r.Err)))
        $r = Invoke-Native $cnExe @('config', 'validate') 60
        Add-Result 'CN path: config validate' ($r.Code -eq 0 -and $r.Out -match '^OK') ("exit {0}" -f $r.Code)
        Test-Doctor $cnExe (Join-Path $CnRoot 'home-doctor') 'CN path:'
        Test-ApiGateway $cnExe (Join-Path $CnRoot 'home-gw') (Join-Path $cnDist '_internal\config') 'CN path:'

        $Timings['smoke tests'] = $sw.Elapsed.TotalSeconds
        $testsFailed = @($Results | Where-Object { $_.Result -eq 'FAIL' }).Count -gt 0
    }

    # ------------------------------------------------------------------ 4. zip + sha256
    if ($testsFailed) {
        Write-Step '4/4 zip skipped: smoke tests failed'
    } else {
        Write-Step '4/4 zip + SHA-256'
        $sw = [Diagnostics.Stopwatch]::StartNew()
        $zip = Join-Path $DistRoot "futrisk-win64-$Version.zip"
        foreach ($f in @($zip, "$zip.sha256")) { if (Test-Path -LiteralPath $f) { Remove-Item -LiteralPath $f -Force } }
        New-Zip $DistDir $zip
        $hash = (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash.ToLowerInvariant()
        [IO.File]::WriteAllText("$zip.sha256", "$hash  $(Split-Path -Leaf $zip)`n", (New-Object System.Text.ASCIIEncoding))
        $Timings['zip'] = $sw.Elapsed.TotalSeconds
        Write-Host ("  {0} ({1:N0} MB)" -f $zip, ((Get-Item -LiteralPath $zip).Length / 1MB))
        Write-Host "  sha256 $hash"
    }
    $Timings['total'] = $total.Elapsed.TotalSeconds
} catch {
    $exitCode = 1
    Write-Host ''
    Write-Host "BUILD FAILED: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.InvocationInfo) { Write-Host "  at $($_.InvocationInfo.PositionMessage)" -ForegroundColor DarkGray }
} finally {
    # processes of this build only: their command lines name our temporary directories
    if (Test-Path -LiteralPath $TempRoot) { Stop-OurProcesses (Split-Path -Leaf $TempRoot) | Out-Null }
    if (Test-Path -LiteralPath $CnRoot) { Stop-OurProcesses $CnName | Out-Null }
    $env:FUTRISK_HOME = $savedHome
    Set-Location $Root
}

if ($Results.Count) {
    Write-Host ''
    Write-Host 'Smoke tests' -ForegroundColor Cyan
    $Results | Format-Table -AutoSize -Wrap | Out-String -Width 220 | Write-Host
}
if ($Timings.Count) {
    Write-Host ('Timings: ' + (($Timings.GetEnumerator() | ForEach-Object { '{0} {1:N0}s' -f $_.Key, $_.Value }) -join ', '))
}
if ($testsFailed) {
    $exitCode = 1
    Write-Host "SMOKE TESTS FAILED: $((@($Results | Where-Object { $_.Result -eq 'FAIL' }) | ForEach-Object { $_.Step }) -join ', ')" -ForegroundColor Red
}
if ($exitCode -ne 0 -or $KeepTemp) {
    if (Test-Path -LiteralPath $TempRoot) { Write-Host "Logs and temporary homes kept in $TempRoot" -ForegroundColor Yellow }
} else {
    foreach ($d in @($TempRoot, $CnRoot)) {
        if (Test-Path -LiteralPath $d) { Remove-Item -LiteralPath $d -Recurse -Force -ErrorAction SilentlyContinue }
    }
}
if ($exitCode -eq 0) { Write-Host 'BUILD OK' -ForegroundColor Green }
exit $exitCode
