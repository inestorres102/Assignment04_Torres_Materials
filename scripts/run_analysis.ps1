# Runs the analysis (tables in results\, figures in figures\). Needs Python 3 with pandas, numpy, matplotlib:
#   python -m pip install -r analysis\requirements.txt
. "$PSScriptRoot\env.ps1"
$py = if (Get-Command python -ErrorAction SilentlyContinue) { 'python' } else { 'py' }
& $py "$ProjectRoot\analysis\analyze.py"
