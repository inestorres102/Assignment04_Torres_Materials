# Verifies the toolchain and writes results\environment.txt (for the report).
. "$PSScriptRoot\env.ps1"
New-Item -ItemType Directory -Force -Path "$ProjectRoot\results" | Out-Null
$f = "$ProjectRoot\results\environment.txt"
$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$os  = Get-CimInstance Win32_OperatingSystem
$rel = (Get-Content "$env:JAVA_HOME\release" | Select-String 'JAVA_VERSION=|IMPLEMENTOR=') -join '; '
$plan = (powercfg /getactivescheme) -join ' '
$batt = (Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue | Select-Object -First 1).BatteryStatus
$lines = @(
  "Date:            $(Get-Date -Format s)",
  "OS:              $($os.Caption) $($os.Version)",
  "CPU:             $($cpu.Name)",
  "Physical cores:  $($cpu.NumberOfCores)   Logical processors: $($cpu.NumberOfLogicalProcessors)",
  "RAM (GB):        $([math]::Round($os.TotalVisibleMemorySize/1MB,1))",
  "Power plan:      $plan",
  "Battery status:  $batt  (2 = on AC power)",
  "JAVA_HOME:       $env:JAVA_HOME  ($rel)",
  "SPARK_HOME:      $env:SPARK_HOME",
  "HADOOP_HOME:     $env:HADOOP_HOME"
)
$lines | Tee-Object -FilePath $f
$sv = Get-ChildItem "$env:SPARK_HOME\jars" -Filter 'spark-core_*.jar' | Select-Object -First 1
Write-Host "Spark core jar:  $($sv.Name)"
Add-Content $f "Spark core jar:  $($sv.Name)"
if (-not $env:HADOOP_HOME) { Write-Host "NOTE: no winutils found (C:\hadoop\bin\winutils.exe). Only needed if Spark reports winutils errors." }
Write-Host "`nOK. Saved to $f"
