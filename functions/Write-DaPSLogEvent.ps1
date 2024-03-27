function Write-DaPSLogEvent {

    <#
    .SYNOPSIS
        A short one-line action-based description, e.g. 'Tests if a function is valid'
    .DESCRIPTION
        A longer description of the function, its purpose, common use cases, etc.
    .NOTES
        Information or caveats about the function e.g. 'This function is not supported in Linux'
    .LINK
        Specify a URI to a help page, this will show when Get-Help -Online is used.
    .EXAMPLE
        Write-DaPSLogEvent "Test Message"

        This just outputs the Log event to the host
    .EXAMPLE
        Write-DaPSLogEvent "F:\DBA\Logs\Logs.log"

        This writes the Log event to a file and displays to the host
    .EXAMPLE
        $Logging = @{

            LogGroupName    = $DataMaskingLogGroup
            LogStreamName   = "Data Masking Process - $($env:COMPUTERNAME) - $(get-date -format "yyyy-MM-dd HH-mm-ss")"
            ProfileName     = $ProfileNameCommon
        }

        Write-DaPSLogEvent "Test Message" @Logging

        This writes the Log event to an AWS Log Stream
    .EXAMPLE
        $Logging = @{

            LogFile         = "F:\DBA\Logs\Logs.log"
            LogGroupName    = $DM.DataMaskingLogGroup
            LogStreamName   = "Data Masking Process - $($env:COMPUTERNAME) - $(get-date -format "yyyy-MM-dd HH-mm-ss")"
            ProfileName     = $ProfileNameCommon
        }

        Write-DaPSLogEvent "Test Message" @Logging

        This writes the Log event to an AWS LogStream and a Logfile as well.
    #>

    [cmdletbinding(DefaultParameterSetName="default")]
    param (

        [parameter(Mandatory,ParameterSetName="default",Position=0)]
        [parameter(Mandatory,ParameterSetName="LogFile",Position=0)]
        [parameter(Mandatory,ParameterSetName="CloudWatch",Position=0)]
        [string]$LogEvent,

        [parameter(ParameterSetName="CloudWatch",Position=1)]
        [parameter(ParameterSetName="LogFile",Position=1)]
        [string]$LogFile,

        [parameter(Mandatory,ParameterSetName="CloudWatch")][string]$LogGroupName,
        [parameter(Mandatory,ParameterSetName="CloudWatch")][string]$LogStreamName,
        [parameter(Mandatory,ParameterSetName="CloudWatch")][string]$ProfileName

    )

    # ------------------------------------------------------ Check if a LogFile has been specified ----------------------------------------------------- #

    if ([string]::IsNullOrEmpty($LogFile)) {

        # ------------------------------- If there isn't a LogFile specified then check if a LogGroup Name has been specified ------------------------------ #

        switch ([string]::IsNullOrEmpty($LogGroupName)) {

            $true {

                # ------------------------------------- if there's no LogFile or LogGroup then just display the Log to the Host ------------------------------------ #

                Write-Host "$(Get-Date -format "dd/MM/yyy HH:mm:ss") : $LogEvent" -ForegroundColor Yellow

            }
            $false {

                # ------------------------------- If there's no LogFile but there is a LogGroup then write the event to the LogStream ------------------------------ #

                $CWEvent            = [Amazon.CloudWatchLogs.Model.InputLogEvent]::new()
                $CWEvent.Timestamp  = Get-Date
                $CWEvent.Message    = $LogEvent

                Write-CWLLogEvent -LogGroupName $LogGroupName -LogStreamName $LogStreamName -LogEvent $CWEvent -ProfileName $ProfileName | Out-Null

                Write-Host "$($CWEvent.Timestamp): $($CWEvent.Message)" -ForegroundColor Green

            }

        }

    }else {

        # -------------------------------- If there is a LogFile specified then check if a LogGroup Name has been specified -------------------------------- #

        switch ([string]::IsNullOrEmpty($LogGroupName)) {

            $true {

                # -------------------------------- If there is a LogFile but there isn't a LogGroup then write the event to the file ------------------------------- #

                $Log = "$(get-date -format "dd/MM/yyy HH:mm:ss") : $LogEvent"
                $Log | out-file -Filepath $LogFile -append -Force
                Write-Host $Log -ForegroundColor Blue

            }
            $false {

                # --------------------------------- If there is a LogFile and LogGroup Name specified then write to both locations --------------------------------- #

                $Log = "$(get-date -format "dd/MM/yyy HH:mm:ss") : $LogEvent"
                $Log | out-file -Filepath $LogFile -append -Force
                Write-Host $Log -ForegroundColor Blue

                # ------------------------------------------------------------ Write event to LogStream ------------------------------------------------------------ #

                $CWEvent            = [Amazon.CloudWatchLogs.Model.InputLogEvent]::new()
                $CWEvent.Timestamp  = Get-Date
                $CWEvent.Message    = $LogEvent

                Write-CWLLogEvent -LogGroupName $LogGroupName -LogStreamName $LogStreamName -LogEvent $CWEvent -ProfileName $ProfileName | Out-Null

                Write-Host "$($CWEvent.Timestamp): $($CWEvent.Message)" -ForegroundColor Green

                # -------------------------------------------------------------------------------------------------------------------------------------------------- #

            }

        }

    }


}