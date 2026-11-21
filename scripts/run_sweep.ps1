# Full strong-scaling sweep. Results: results\raw_spark.csv and results\config_status.csv
param(
    [int[]]$WorkersList = @(1, 2, 4, 8, 12, 16),
    [int[]]$Sizes = @(250, 500, 750, 1000, 2000, 4000),
    [int]$Warmup = 3,
    [int]$Reps = 10,
    [int]$TimeoutSec = 900,          # per configuration (warm-ups + repetitions)
    [int]$BlocksPerWorker = 4,       # RDD partitions = workers * BlocksPerWorker (capped at n)
    [int]$MaxWorkersFor4000 = 8,     # declared memory budget: n >= 4000 only up to this many workers
    [int]$ExecBudgetMb = 6000        # total executor heap budget, split among workers
)
. "$PSScriptRoot\lib.ps1"
New-Item -ItemType Directory -Force -Path "$ProjectRoot\results", "$ProjectRoot\logs" | Out-Null
$out = "$ProjectRoot\results\raw_spark.csv"
$st  = "$ProjectRoot\results\config_status.csv"

foreach ($w in $WorkersList) {
    $mem = [int][math]::Min(2048, [math]::Max(512, [math]::Floor($ExecBudgetMb / $w)))
    & "$PSScriptRoot\cluster.ps1" -Action start -Workers $w -MemMb $mem
    foreach ($n in $Sizes) {
        if ($n -ge 4000 -and $w -gt $MaxWorkersFor4000) {
            Write-Host "n=$n workers=$w : skipped (declared memory budget)"
            Add-StatusRow -Csv $st -Line "$n,$w,,$mem,,,,0,,SKIPPED_MEMORY_BUDGET"
            continue
        }
        $blocks = [int][math]::Min($n, $w * $BlocksPerWorker)
        $s = Invoke-Bench -N $n -Workers $w -Blocks $blocks -MemMb $mem -Warmup $Warmup -Reps $Reps `
                          -Out $out -StatusCsv $st -TimeoutSec $TimeoutSec
        Start-Sleep -Seconds 3
    }
}
& "$PSScriptRoot\cluster.ps1" -Action stop
Write-Host "Sweep finished. See results\raw_spark.csv and results\config_status.csv"
