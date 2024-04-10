function New-DaPSLogGroupLogStream {
    param (

        [string]$LogGroupName,
        [string]$LogStreamName,
        [string]$ProfileName
    )

    if (!(Get-CWLLogGroup -LogGroupNamePrefix $LogGroupName -ProfileName $ProfileName)) {

        New-CWLLogGroup -LogGroupName $LogGroupName -ProfileName $ProfileName
        Write-DaPSLogEvent "[AWS] LogGroup Created - [$($LogGroupName)]"
        Start-Sleep 2
    }

    if (!(Get-CWLLogStream -LogGroupName $LogGroupName -LogStreamNamePrefix $LogStreamName -ProfileName $ProfileName)) {

        New-CWLLogStream -LogGroupName $LogGroupName -LogStreamName $LogStreamName -ProfileName $ProfileName
        Write-DaPSLogEvent "[AWS] LogStream Created - [$LogStreamName]"
        Start-Sleep 3
    }

}