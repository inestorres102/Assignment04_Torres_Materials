# CONTROL EXPERIMENT: does the Spark plateau come from memory traffic of the unblocked i-k-j kernel?
# Only the kernel changes (ikj vs cache-blocked); everything else is identical.
#   Part 1: thread pool, same kernel and same task granularity (4 tasks per thread) as Spark
#   Part 2: Spark standalone, for each (n, workers) ikj runs first, then blocked, in the same cluster session
# Output: results\raw_control_threads.csv, results\raw_control_spark.csv, results\control_*status.csv
param(
    [int[]]$Sizes = @(2000, 4000),
    [int[]]$WorkersList = @(1, 2, 4, 8, 12, 16),
    [string[]]$Kernels = @('ikj', 'blocked'),
    [string]$ThreadKernels = 'blocked',
    [int]$Warmup = 3,
    [int]$Reps = 10,
    [int]$TimeoutSec = 2400,
    [int]$BlocksPerWorker = 4,
    [int]$MaxWorkersFor4000 = 8,
    [int]$ExecBudgetMb = 6000,
    [int]$KB = 64,
    [int]$JB = 256,
    [switch]$SkipThreads,
    [switch]$SkipSpark
)
. "$PSScriptRoot\lib.ps1"
$classes = "$ProjectRoot\target\classes"
if (-not (Test-Path "$classes\es\ulpgc\bigdata\a4\ControlBaseline.class")) { throw 'Run scripts\build.ps1 first.' }
New-Item -ItemType Directory -Force -Path "$ProjectRoot\results", "$ProjectRoot\logs" | Out-Null
$outT = "$ProjectRoot\results\raw_control_threads.csv"
$outS = "$ProjectRoot\results\raw_control_spark.csv"
$stT  = "$ProjectRoot\results\control_threads_status.csv"
$stS  = "$ProjectRoot\results\control_spark_status.csv"
$tl = '1,2,4,8,12,16'

if (-not $SkipThreads) {
    if (-not (Test-Path $stT)) { Add-Content $stT 'n,peak_working_set_mb,exit_code,status' }
    foreach ($n in $Sizes) {
        Write-Host ("[{0}] control thread pool n={1} kernels={2} ..." -f (Get-Date -Format 'HH:mm:ss'), $n, $ThreadKernels)
        $log = "$ProjectRoot\logs\control_threads_n$n.out"
        $err = "$ProjectRoot\logs\control_threads_n$n.err"
        $ja = @('-cp', $classes, 'es.ulpgc.bigdata.a4.ControlBaseline', '--n', "$n", '--warmup', "$Warmup",
                '--reps', "$Reps", '--threads', $tl, '--kernels', $ThreadKernels,
                '--tasks-per-thread', "$BlocksPerWorker", '--kb', "$KB", '--jb', "$JB", '--out', $outT)
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
        Add-Content $stT ("{0},{1},{2},{3}" -f $n, (Format-Inv ($peak / 1MB)), $rc, $status)
        Write-Host ("    -> {0} (exit {1}), peak working set {2} MB" -f $status, $rc, (Format-Inv ($peak / 1MB)))
    }
}

if (-not $SkipSpark) {
    foreach ($w in $WorkersList) {
        $mem = [int][math]::Min(2048, [math]::Max(512, [math]::Floor($ExecBudgetMb / $w)))   # same rule as run_sweep
        & "$PSScriptRoot\cluster.ps1" -Action start -Workers $w -MemMb $mem
        foreach ($n in $Sizes) {
            if ($n -ge 4000 -and $w -gt $MaxWorkersFor4000) {
                Write-Host "n=$n workers=$w : skipped (declared memory budget)"
                Add-StatusRow -Csv $stS -Line "$n,$w,,$mem,,,,0,,SKIPPED_MEMORY_BUDGET,all"
                continue
            }
            $blocks = [int][math]::Min($n, $w * $BlocksPerWorker)
            foreach ($k in $Kernels) {
                $s = Invoke-Bench -N $n -Workers $w -Blocks $blocks -MemMb $mem -Warmup $Warmup -Reps $Reps `
                                  -Out $outS -StatusCsv $stS -TimeoutSec $TimeoutSec -Kernel $k -KB $KB -JB $JB
                Start-Sleep -Seconds 3
            }
        }
    }
    & "$PSScriptRoot\cluster.ps1" -Action stop
}
Write-Host 'Control experiment finished. See results\raw_control_*.csv and results\control_*status.csv'
