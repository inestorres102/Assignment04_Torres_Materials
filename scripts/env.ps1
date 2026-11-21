# Common environment for all scripts. Dot-source it:  . "$PSScriptRoot\env.ps1"
# Edit the two override variables below only if auto-detection fails.
#   $env:A4_JAVA_HOME  -> folder of a JDK 21 (contains bin\java.exe)
#   $env:A4_SPARK_HOME -> folder of Spark 4.1.x (contains bin\spark-submit.cmd)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
if ($ProjectRoot -match '\s') { throw "The project path must not contain spaces: $ProjectRoot" }

# ---- JDK 21 -------------------------------------------------------------
$jdk = $null
if ($env:A4_JAVA_HOME -and (Test-Path "$($env:A4_JAVA_HOME)\bin\java.exe")) {
    $jdk = $env:A4_JAVA_HOME
} else {
    $cands = @()
    foreach ($base in @('C:\Program Files\Eclipse Adoptium', 'C:\Program Files\Java',
                        'C:\Program Files\Microsoft', 'C:\Program Files\Amazon Corretto',
                        'C:\Program Files\Zulu')) {
        if (Test-Path $base) { $cands += Get-ChildItem $base -Directory -Filter '*21*' }
    }
    $pick = $cands | Where-Object { Test-Path "$($_.FullName)\bin\java.exe" } |
            Sort-Object Name -Descending | Select-Object -First 1
    if ($pick) { $jdk = $pick.FullName }
}
if (-not $jdk) {
    throw "JDK 21 not found. Install it (winget install EclipseAdoptium.Temurin.21.JDK) or set `$env:A4_JAVA_HOME."
}

# ---- Spark ---------------------------------------------------------------
$sparkHome = if ($env:A4_SPARK_HOME) { $env:A4_SPARK_HOME } else { 'C:\spark\spark-4.1.4-bin-hadoop3' }
if (-not (Test-Path "$sparkHome\bin\spark-submit.cmd")) {
    throw "Spark not found at $sparkHome. Extract it there or set `$env:A4_SPARK_HOME."
}

# ---- Environment for this PowerShell process (inherited by child processes) --
$env:JAVA_HOME = $jdk
$env:SPARK_HOME = $sparkHome
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:PATH = "$jdk\bin;$sparkHome\bin;$env:PATH"

# Optional: winutils for Hadoop on Windows (only needed if Spark logs errors about it)
if (Test-Path 'C:\hadoop\bin\winutils.exe') { $env:HADOOP_HOME = 'C:\hadoop' }
