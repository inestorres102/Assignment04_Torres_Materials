# Compile against the jars shipped with Spark and package target\a4.jar (no Maven needed).
. "$PSScriptRoot\env.ps1"
$classes = "$ProjectRoot\target\classes"
Remove-Item "$ProjectRoot\target" -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $classes | Out-Null

$src = Get-ChildItem "$ProjectRoot\src" -Recurse -Filter *.java | ForEach-Object { $_.FullName }
& "$env:JAVA_HOME\bin\javac.exe" --release 21 -encoding UTF-8 -cp "$env:SPARK_HOME\jars\*" -d $classes $src
if ($LASTEXITCODE -ne 0) { throw 'javac failed' }
& "$env:JAVA_HOME\bin\jar.exe" --create --file "$ProjectRoot\target\a4.jar" -C $classes .
if ($LASTEXITCODE -ne 0) { throw 'jar failed' }
Write-Host "Built $ProjectRoot\target\a4.jar"
