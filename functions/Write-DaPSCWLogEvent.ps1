<#
.NOTES
    Check and create LogGroup or LogStream

    if (!(Get-CWLLogGroup -LogGroupNamePrefix $LogGroupName -ProfileName $ProfileName)) {

        New-CWLLogGroup -LogGroupName $LogGroupName -ProfileName $ProfileName
        Write-Host "LogGroup Created - [$LogGroupName]"
        Start-Sleep 1
    }

    if (!(Get-CWLLogStream -LogGroupName $LogGroupName -LogStreamNamePrefix $LogStreamName -ProfileName $ProfileName)) {

        New-CWLLogStream -LogGroupName $LogGroupName -LogStreamName $LogStreamName -ProfileName $ProfileName
        Write-Host "LogStream Created - [$LogStream]"
    }

#>

function Write-DaPSCWLogEvent {
    param (

        [string]$LogEvent,
        [string]$LogGroupName   = $LogGroup,
        [string]$LogStreamName  = $LogStream,
        [string]$ProfileName    = $ProfileName

    )

    $CWEvent            = [Amazon.CloudWatchLogs.Model.InputLogEvent]::new()
    $CWEvent.Timestamp  = Get-Date
    $CWEvent.Message    = $LogEvent

    Write-CWLLogEvent -LogGroupName $LogGroupName -LogStreamName $LogStreamName -LogEvent $CWEvent -ProfileName $ProfileName | Out-Null

    Write-Host "$($CWEvent.Timestamp): $($CWEvent.Message)" -ForegroundColor Green

}