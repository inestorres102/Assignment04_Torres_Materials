# Helpers: run one benchmark configuration with a resource sampler.
. "$PSScriptRoot\env.ps1"

function Start-Sampler {
    param([string]$File)
    Start-Job -ArgumentList $File -ScriptBlock {
        param($File)
        while ($true) {
            $tot = 0.0; $mxPeak = 0.0; $drvPeak = 0.0; $cnt = 0
            Get-CimInstance Win32_Process -Filter "Name='java.exe'" | ForEach-Object {
                $c = $_.CommandLine
                if ($c -match 'CoarseGrainedExecutorBackend') {
                    $tot += [double]$_.WorkingSetSize          # bytes
                    $cnt++
                    if ([double]$_.PeakWorkingSetSize -gt $mxPeak) { $mxPeak = [double]$_.PeakWorkingSetSize }   # KB
                } elseif ($c -match 'deploy\.SparkSubmit') {
                    if ([double]$_.PeakWorkingSetSize -gt $drvPeak) { $drvPeak = [double]$_.PeakWorkingSetSize } # KB
                }
            }
            "$tot,$mxPeak,$drvPeak,$cnt" | Add-Content -Path $File
            Start-Sleep -Milliseconds 300
        }
    }
}

function Format-Inv([double]$v) {
    return [math]::Round($v, 1).ToString([System.Globalization.CultureInfo]::InvariantCulture)
}

function Add-StatusRow {
    param([string]$Csv, [string]$Line)
    if (-not (Test-Path $Csv)) {
        Add-Content -Path $Csv -Value 'n,workers,blocks,mem_per_executor_mb,peak_total_exec_ws_mb,peak_max_exec_ws_mb,peak_driver_ws_mb,samples,exit_code,status,kernel'
    }
    Add-Content -Path $Csv -Value $Line
}

function Invoke-Bench {
    param(
        [int]$N, [int]$Workers, [int]$Blocks, [int]$MemMb,
        [int]$Warmup, [int]$Reps, [string]$Out, [string]$StatusCsv,
        [int]$TimeoutSec = 900, [string]$Master = 'spark://127.0.0.1:7077',
        [string]$Kernel = 'ikj', [int]$KB = 64, [int]$JB = 256
    )
    $tag = "n${N}_w${Workers}_${Kernel}"
    $log = "$ProjectRoot\logs\$tag.out"
    $err = "$ProjectRoot\logs\$tag.err"
    $samples = "$ProjectRoot\logs\$tag.rss"
    if (Test-Path $samples) { Remove-Item $samples -Force }

    $sa = @('--master', $Master,
            '--class', 'es.ulpgc.bigdata.a4.Bench',
            '--executor-cores', '1',
            '--total-executor-cores', "$Workers",
            '--executor-memory', "${MemMb}m",
            '--driver-memory', '3g',
            '--conf', 'spark.rpc.message.maxSize=512',
            '--conf', 'spark.driver.maxResultSize=2g',
            "$ProjectRoot\target\a4.jar",
            '--n', "$N", '--workers', "$Workers", '--blocks', "$Blocks",
            '--warmup', "$Warmup", '--reps', "$Reps", '--out', $Out,
            '--kernel', $Kernel, '--kb', "$KB", '--jb', "$JB")

    Write-Host ("[{0}] n={1} workers={2} blocks={3} kernel={4} mem/exec={5}MB ..." -f (Get-Date -Format 'HH:mm:ss'), $N, $Workers, $Blocks, $Kernel, $MemMb)
    $p = Start-Process -FilePath "$env:SPARK_HOME\bin\spark-submit.cmd" -ArgumentList $sa `
         -NoNewWindow -PassThru -RedirectStandardOutput $log -RedirectStandardError $err
    $null = $p.Handle
    $job = Start-Sampler -File $samples

    $finished = $p.WaitForExit($TimeoutSec * 1000)
    if (-not $finished) {
        & taskkill /PID $p.Id /T /F | Out-Null
        $rc = 124; $status = 'TIMEOUT'
    } else {
        $rc = $p.ExitCode
        $status = if ($rc -eq 0) { 'OK' } else { 'FAILED' }
    }
    Stop-Job $job -ErrorAction SilentlyContinue
    Remove-Job $job -Force -ErrorAction SilentlyContinue

    $peakTot = ''; $peakMx = ''; $peakDrv = ''; $cnt = 0
    if (Test-Path $samples) {
        $rows = @(Get-Content $samples | Where-Object { $_ } | ForEach-Object {
            $q = $_ -split ','
            [pscustomobject]@{ tot = [double]$q[0]; mx = [double]$q[1]; drv = [double]$q[2] }
        })
        $cnt = $rows.Count
        if ($cnt -gt 0) {
            $peakTot = Format-Inv ((($rows | Measure-Object tot -Maximum).Maximum) / 1MB)
            $peakMx  = Format-Inv ((($rows | Measure-Object mx  -Maximum).Maximum) / 1024)
            $peakDrv = Format-Inv ((($rows | Measure-Object drv -Maximum).Maximum) / 1024)
        }
    }
    Add-StatusRow -Csv $StatusCsv -Line "$N,$Workers,$Blocks,$MemMb,$peakTot,$peakMx,$peakDrv,$cnt,$rc,$status,$Kernel"
    Write-Host ("    -> {0} (exit {1}); peak executors total={2} MB, driver={3} MB" -f $status, $rc, $peakTot, $peakDrv)
    return $status
}
