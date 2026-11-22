# Re-measures the A3 sequential and thread-pool baselines on this machine (JDK 21), same inputs as Spark.
# Output: results\raw_baseline.csv and results\baseline_status.csv (peak working set per size)
param(
    [int[]]$Sizes = @(250, 500, 750, 1000, 2000, 4000),
    [int]$Warmup = 3,
    [int]$Reps = 10,
    [int]$TimeoutSec = 3600,
    [string]$Threads = '1,2,4,8,12,16'
)
. "$PSScriptRoot\lib.ps1"
$classes = "$ProjectRoot\target\classes"
if (-not (Test-Path "$classes\es\ulpgc\bigdata\a4\Baseline.class")) { throw 'Run scripts\build.ps1 first.' }
New-Item -ItemType Directory -Force -Path "$ProjectRoot\results", "$ProjectRoot\logs" | Out-Null
$out = "$ProjectRoot\results\raw_baseline.csv"
$st  = "$ProjectRoot\results\baseline_status.csv"
if (-not (Test-Path $st)) { Add-Content $st 'n,peak_working_set_mb,exit_code,status' }

foreach ($n in $Sizes) {
    Write-Host ("[{0}] baseline n={1} ..." -f (Get-Date -Format 'HH:mm:ss'), $n)
    $log = "$ProjectRoot\logs\baseline_n$n.out"
    $err = "$ProjectRoot\logs\baseline_n$n.err"
    $ja = @('-cp', $classes, 'es.ulpgc.bigdata.a4.Baseline', '--n', "$n", '--warmup', "$Warmup",
            '--reps', "$Reps", '--threads', $Threads, '--out', $out)
    $p = Start-Process -FilePath "$env:JAVA_HOME\bin\java.exe" -ArgumentList $ja -NoNewWindow -PassThru `
         -RedirectStandardOutput $log -RedirectStandardError $err
    $null = $p.Handle
    $peak = 0.0
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while (-not $p.HasExited -and (Get-Date) -lt $deadline) {
        try { $p.Refresh(); if ([double]$p.PeakWorkingSet64 -gt $peak) { $peak = [double]$p.PeakWorkingSet64 } } catch { }
        Start-Sleep -Milliseconds 200
    }
    if (-not $p.HasExited) { & taskkill /PID $p.Id /T /F | Out-Null; $rc = 124; $status = 'TIMEOUT' }
    else { $rc = $p.ExitCode; $status = if ($rc -eq 0) { 'OK' } else { 'FAILED' } }
    Add-Content $st ("{0},{1},{2},{3}" -f $n, (Format-Inv ($peak / 1MB)), $rc, $status)
    Write-Host ("    -> {0} (exit {1}), peak working set {2} MB" -f $status, $rc, (Format-Inv ($peak / 1MB)))
}
Write-Host 'Baseline finished. See results\raw_baseline.csv and results\baseline_status.csv'
