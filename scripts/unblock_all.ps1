# Run once after unzipping if PowerShell refuses to run the scripts.
Get-ChildItem (Split-Path -Parent $PSScriptRoot) -Recurse | Unblock-File
Write-Host 'Done.'
