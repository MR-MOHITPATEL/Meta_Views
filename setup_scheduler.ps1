# PowerShell Script to Setup Windows Task Scheduler for Meta Pipeline
# Run this script as Administrator to ensure tasks are created correctly.

$ActionScript = "run_all.py"
$WorkingDir = "c:\Users\mahar\Documents\Projects\ZenJeevani\Master Data"
$VenvPython = "$WorkingDir\.venv\Scripts\python.exe"

if (-not (Test-Path $VenvPython)) {
    Write-Host "Error: Virtual environment python not found at $VenvPython" -ForegroundColor Red
    Write-Host "Please ensure your .venv is set up correctly in the project root."
    exit
}

$Times = @("09:00:00", "15:00:00", "18:00:00")
$TaskBaseName = "Meta_Ads_Pipeline"

foreach ($Time in $Times) {
    $TaskName = "$TaskBaseName`_$($Time.Replace(':', ''))"
    Write-Host "Creating task: $TaskName at $Time"
    
    $Action = New-ScheduledTaskAction -Execute $VenvPython -Argument $ActionScript -WorkingDirectory $WorkingDir
    $Trigger = New-ScheduledTaskTrigger -Daily -At $Time
    $Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
    
    # Check if task already exists
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }
    
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Description "Automated Meta Ads Performance and Pincode Pipeline"
}

Write-Host "All tasks successfully created! You can verify them in Task Scheduler (taskschd.msc)." -ForegroundColor Green
