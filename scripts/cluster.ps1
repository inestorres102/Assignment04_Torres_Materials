# Start/stop a local Spark standalone cluster: 1 master + N workers (1 core each).
param(
    [ValidateSet('start', 'stop')][string]$Action = 'start',
    [int]$Workers = 2,
    [int]$MemMb = 1024
)
. "$PSScriptRoot\env.ps1"

function Stop-SparkProcesses {
    Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
        Where-Object { $_.CommandLine -match 'org\.apache\.spark\.(deploy\.(master\.Master|worker\.Worker|SparkSubmit)|executor\.CoarseGrainedExecutorBackend)' } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

Stop-SparkProcesses
if ($Action -eq 'stop') { Write-Host 'Spark processes stopped.'; return }

Start-Sleep -Seconds 2
New-Item -ItemType Directory -Force -Path "$ProjectRoot\logs" | Out-Null
Remove-Item "$ProjectRoot\work" -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$ProjectRoot\work" | Out-Null

$env:SPARK_DAEMON_MEMORY = '256m'
$cls = "$env:SPARK_HOME\bin\spark-class.cmd"

Start-Process -FilePath $cls -WindowStyle Hidden `
    -ArgumentList @('org.apache.spark.deploy.master.Master', '--host', '127.0.0.1', '--port', '7077', '--webui-port', '8080') `
    -RedirectStandardOutput "$ProjectRoot\logs\master.out" -RedirectStandardError "$ProjectRoot\logs\master.err"

# wait until the master accepts connections
$ok = $false
for ($t = 0; $t -lt 60 -and -not $ok; $t++) {
    try { $c = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 7077); $c.Close(); $ok = $true }
    catch { Start-Sleep -Milliseconds 500 }
}
if (-not $ok) { throw "Master did not start. See $ProjectRoot\logs\master.err" }

for ($i = 1; $i -le $Workers; $i++) {
    $wport = 8080 + $i
    Start-Process -FilePath $cls -WindowStyle Hidden `
        -ArgumentList @('org.apache.spark.deploy.worker.Worker', '--host', '127.0.0.1',
                        '--webui-port', "$wport", '--cores', '1', '--memory', "${MemMb}m",
                        '--work-dir', "$ProjectRoot\work\w$i", 'spark://127.0.0.1:7077') `
        -RedirectStandardOutput "$ProjectRoot\logs\worker$i.out" -RedirectStandardError "$ProjectRoot\logs\worker$i.err"
}

# wait until all workers are registered
$alive = 0
for ($t = 0; $t -lt 120 -and $alive -lt $Workers; $t++) {
    try {
        $j = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/json/' -TimeoutSec 3
        $alive = @($j.workers | Where-Object { $_.state -eq 'ALIVE' }).Count
    } catch { }
    if ($alive -lt $Workers) { Start-Sleep -Milliseconds 500 }
}
Write-Host "Cluster: $alive / $Workers workers ALIVE (1 core, ${MemMb} MB each). Web UI: http://localhost:8080"
if ($alive -lt $Workers) { Write-Warning "Not all workers registered. See $ProjectRoot\logs\worker*.err" }
