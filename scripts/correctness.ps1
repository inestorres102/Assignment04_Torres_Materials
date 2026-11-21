# Correctness tests (identity, zero, distributed vs sequential) in Spark local mode.
. "$PSScriptRoot\env.ps1"
& "$env:SPARK_HOME\bin\spark-submit.cmd" --master 'local[2]' --conf spark.ui.enabled=false `
    --class es.ulpgc.bigdata.a4.Correctness "$ProjectRoot\target\a4.jar" 2>&1 |
    ForEach-Object { "$_" } | Where-Object { $_ -match 'PASS|FAIL|checks|ERROR|Exception' } |
    Tee-Object -FilePath "$ProjectRoot\results\correctness.txt" | Select-Object -Last 15
Write-Host "Full list of checks: results\correctness.txt  (look for the final line '... checks, 0 failures')"
