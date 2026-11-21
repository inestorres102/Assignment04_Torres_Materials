# Quick end-to-end test: 2 workers, n=500, 1 warm-up, 2 repetitions.
. "$PSScriptRoot\lib.ps1"
New-Item -ItemType Directory -Force -Path "$ProjectRoot\results" | Out-Null
$out = "$ProjectRoot\results\smoke.csv"
$st  = "$ProjectRoot\results\smoke_status.csv"
Remove-Item $out, $st -ErrorAction SilentlyContinue

& "$PSScriptRoot\cluster.ps1" -Action start -Workers 2 -MemMb 1024
$s = Invoke-Bench -N 500 -Workers 2 -Blocks 8 -MemMb 1024 -Warmup 1 -Reps 2 -Out $out -StatusCsv $st -TimeoutSec 300
& "$PSScriptRoot\cluster.ps1" -Action stop

Write-Host "`nStatus: $s"
if (Test-Path $out) { Import-Csv $out | Format-Table phase, rep, executors_observed, blocks, t_bcast_create_s, t_job_s, t_e2e_s, kernel_sum_s, max_abs_diff -AutoSize }
if ($s -ne 'OK') { Write-Host "Check logs\n500_w2.err and logs\master.err / logs\worker1.err" }
