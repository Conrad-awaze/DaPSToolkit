function Reset-DaPSSDLCEnvironment {
    param (

        [string]$SQLInstance,
        [string]$Database,
        [string]$ExcelfileFolder,
        [string]$ddbParametersTable,
        [string]$ProfileNameCommon

    )

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                     PARAMETERS                                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Parameters

    $keyEnvRefresh          = @{ PK = 'SDLC'; SK = 'EnvironmentRefresh'} | ConvertTo-DDBItem
    $Params                 = Get-DDBItem -TableName $ddbParametersTable -Key $keyEnvRefresh -ProfileName $ProfileNameCommon | ConvertFrom-DDBItem
    #$Params                 = Import-Excel -Path $Excelfile -WorksheetName 'Parameters-EnvironmentRefresh'

    $Excelfile              = (Get-ChildItem $ExcelfileFolder | Where-Object Extension -EQ '.xlsx'| Sort-Object $_.CreationTime | Select-Object  -First 1).FullName
    $Env                    = Import-Excel -Path $ExcelFile -WorksheetName 'Parameters -Traveller Refresh' | Where-Object {$_.Database -eq $Database}
    $AllUserAccounts        = Import-Excel -Path $ExcelFile -WorksheetName 'Database Access' | Where-Object {($_.Database -eq $Database) -and ($_.SQL_Instance -eq $SQLInstance)}
    $Teams                  = Import-Excel -Path $ExcelFile -WorksheetName 'Teams'
    $EnvILT                 = Import-Excel -Path $ExcelFile -WorksheetName 'Parameters - Database Refresh' | Where-Object {$_.Database -eq $($Env.ILTDatabase)}
    $EnvILTSILT             = Import-Excel -Path $ExcelFile -WorksheetName 'Parameters - Database Refresh' | Where-Object {$_.Database -eq $($Env.ILTDatabaseSILT)}

    $Logging = @{

        LogGroupName    = $Params.LogGroup
        LogStreamName   = "Environment Refresh - $Database - $(get-date -format "yyyy-MM-dd HH-mm-ss")"
        ProfileName     = $ProfileNameCommon
    }

    # ------------------------------------------------------------------ Refresh Files ----------------------------------------------------------------- #
    #region Refresh Files

    $TravellerReplicationILTLive            = "$($Params.'Folder-ReplicationScriptsLive')\$($Params.'Script-TravellerReplicationLiveILT')"
    $TravellerReplicationSupplierExtrasLive = "$($Params.'Folder-ReplicationScriptsLive')\$($Params.'Script-TravellerReplicationLiveSupplierExtras')"
    $SILTReplicationILTLive                 = "$($Params.'Folder-ReplicationScriptsLive')\$($Params.'Script-ReplicationSILTILT')"
    $SILTReplicationAvailabilityCacheLive   = "$($Params.'Folder-ReplicationScriptsLive')\$($Params.'Script-ReplicationSILTAvailabilityCache')"
    $FolderReplicationScriptsSDLC           = "$($Params.'Folder-ReplicationScriptsSDLC')\$Database"

    $TravellerReplicationILTSDLC            = "$FolderReplicationScriptsSDLC\Traveller Replication ILT $($Database).sql"
    $TravellerReplicationSupplierExtrasSDLC = "$FolderReplicationScriptsSDLC\Traveller Replication Supplier Extras $($Database).sql"
    $SILTReplicationILTSDLC                 = "$FolderReplicationScriptsSDLC\SILT Replication ILT $($Env.ILTDatabaseSILT).sql"
    $SILTReplicationAvailabilityCacheSDLC   = "$FolderReplicationScriptsSDLC\SILT Replication Availability Cache $($Env.ILTDatabaseSILT).sql"

    $IndexCreateScriptSupplierExtra = "$($Params.RefreshFilesFolder)\$($Params.'Script-IndexCreate-SupplierExtra')"
    $IndexCreateScript              = "$($Params.RefreshFilesFolder)\$($Params.'Script-IndexCreate')"
    $ViewCreateScript               = "$($Params.RefreshFilesFolder)\$($Params.'Script-ViewCreate')"
    $IndexList                      = "$($Params.RefreshFilesFolder)\$($Params.IndexList)"
    $ILTView                        = "$($Params.RefreshFilesFolder)\$($Params.ILTView)"
    $TravellerConfigScripts         = Get-ChildItem -Path $Env.TravellerConfigScripts

    #endregion

    # ------------------------------------------------------------- Environment Permissions ------------------------------------------------------------ #

    $UserAccountsDBO                = ($AllUserAccounts | Where-Object {$_.Permissions -eq 'DBOwner'}).User
    $UserAccountsReadAccess         = ($AllUserAccounts | Where-Object {$_.Permissions -eq 'READ'}).User
    $UserAccountsWritePermissions   = ($AllUserAccounts | Where-Object {$_.Permissions -eq 'Write'}).User
    $UserAccountsExecutePermissions = ($AllUserAccounts | Where-Object {$_.Permissions -eq 'Execute'}).User
    $UserAccountsBulkAdmin          = ($AllUserAccounts | Where-Object {$_.Permissions -eq "bulkadmin"}).User

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #

    $TimerEnvironmentRefresh    = 'EnvironmentRefresh'
    $RegexDatabaseName          = '(?<=\[)[^]]+(?=\])' # Extracts Database Name Between brackets [] from the Restore SQL command
    $RegexBackupFile            = '[^\\]*\.(bak|trn|diff|DIFF)'
    $TeamsSendInterval          = 9
    $global:CountRestore        = 0

    $URI = "https://awazecom.webhook.office.com/webhookb2/$($Teams.TeamsGUID1_Refresh_Environment)/IncomingWebhook/$($Teams.TeamsGUID2_Refresh_Environment)"

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                      FUNCTIONS                                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Functions
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
    function Remove-DaPSReplication {
        [CmdletBinding()]
        param (
            [string]$SQLInstance,
            [string]$TravellerDatabase,
            [string]$PublicationName,
            $Logging
        )

        begin {


            $Query = "Select publisher_db,publication from Distribution.dbo.MSpublications"
            $ReplicationDetails = Invoke-DbaQuery -SqlInstance $SQLInstance -Query $Query -As PSObject

            $DropReplication          = "
                EXEC sp_dropsubscription
                @publication = N'$PublicationName',
                @article = N'all',
                @subscriber = N'all';

                EXEC sp_droppublication @publication = N'$PublicationName';
            "

        }

        process {

            if ($($ReplicationDetails.publication).Contains($PublicationName)) {

                try {

                    # If Publication exists then remove it
                    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $TravellerDatabase -Query $DropReplication -Verbose -ErrorAction Stop -WarningAction Stop -WarningVariable CapturedWarning

                }
                catch {

                    Write-DaPSLogEvent "Error removing [$PublicationName] publication. Retrying...!!!" @Logging
                    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $TravellerDatabase -Query $DropReplication -Verbose

                }

                # Recheck if the publication has been removed
                $ReplicationDetails = Invoke-DbaQuery -SqlInstance $SQLInstance -Query $Query -As PSObject

                # Check for NULLS and assign a value
                if ([string]::IsNullOrEmpty($ReplicationDetails)) {

                    $ReplicationDetails = 'NoPublications'
                }

                if ($($ReplicationDetails.publication).Contains($PublicationName)) {

                    $PublicationCheckResult =  "Publication $PublicationName has not been removed. Please investigate...!!!"
                    Break
                }
                else {

                    $PublicationCheckResult = "Removed Publication [$PublicationName] from $TravellerDatabase"

                }

            }
            else {

                $PublicationCheckResult = "The Publication [$PublicationName] is not present in the $TravellerDatabase Database."

            }
        }

        end {

            $PublicationCheckResult

        }
    }
    function Wait-DaPSDatabaseRefreshCheck {

        [OutputType([void])]
        [CmdletBinding()]
        param
        (
            [Parameter(Mandatory)]
            [ValidateNotNullOrEmpty()]
            [scriptblock]$Condition,

            [Parameter(Mandatory)]
            [ValidateNotNullOrEmpty()]
            [int]$Timeout,

            [Parameter()]
            [ValidateNotNullOrEmpty()]
            [object[]]$ArgumentList,
            # $ArgumentList,

            [Parameter()]
            [ValidateNotNullOrEmpty()]
            [int]$RetryInterval,

            $Logging
        )
        try {

            # https://mcpmag.com/articles/2018/03/16/wait-action-function-powershell.aspx

            $timer = [Diagnostics.Stopwatch]::StartNew()

            while (($timer.Elapsed.TotalSeconds -lt $Timeout) -and ((& $Condition $ArgumentList) -eq $true)) {

                # ------------------------------------ While the Check Returns True query the Instance for the Current Progress ------------------------------------ #

                $SQLMonitorScript = @"

                    SELECT r.session_id AS [Session_Id]
                    ,r.command AS [Command]
                    ,CONVERT(NUMERIC(6, 2), r.percent_complete) AS [Percentage_Complete]
                    ,GETDATE() AS [Current_Time]
                    ,CONVERT(VARCHAR(20), DATEADD(ms, r.estimated_completion_time, GetDate()), 20) AS [Estimated_Completion_Time]
                    ,CONVERT(NUMERIC(32, 2), r.total_elapsed_time / 1000.0 / 60.0) AS [Elapsed_Min]
                    ,CONVERT(NUMERIC(32, 2), r.estimated_completion_time / 1000.0 / 60.0) AS [Estimated_Min]
                    ,CONVERT(NUMERIC(32, 2), r.estimated_completion_time / 1000.0 / 60.0 / 60.0) AS [Estimated_Hours]
                    ,CONVERT(VARCHAR(1000), (
                        SELECT SUBSTRING(TEXT, r.statement_start_offset / 2, CASE
                            WHEN r.statement_end_offset = - 1
                                THEN 1000
                            ELSE (r.statement_end_offset - r.statement_start_offset) / 2
                            END) 'Statement text'
                        FROM sys.dm_exec_sql_text(sql_handle)
                        )) as [SQL_Statement]
                    FROM sys.dm_exec_requests r
                    WHERE command like 'RESTORE%' --or command like 'BACKUP%'

"@

                $RestoreStatus  = Invoke-DbaQuery -SqlInstance "$($ArgumentList.SQLInstance)" -Database Master -Query $SQLMonitorScript -As PSObject

                # ---------------------------------------------- For Each Record Returned Output the Current Progress ---------------------------------------------- #

                $RestoreStatusResults = @()
                $RestoreStatus | ForEach-Object {

                    if ($_.SQL_Statement) {

                        $Database   = ([regex]::Matches("$($_.SQL_Statement)" , $($ArgumentList.RegexDatabaseName))).Value
                        $File       = ([regex]::Matches("$($_.SQL_Statement)" , $($ArgumentList.RegexBackupFile))).Value

                        $StatusSnapshot = [PSCustomObject]@{

                            Database        = $Database
                            PercentComplete = "$($_.Percentage_Complete)%"
                            BackupFile      = $File
                        }

                        $RestoreStatusResults += $StatusSnapshot

                    }
                }

                $RestoreStatusResults | ForEach-Object {

                    Write-DaPSLogEvent "Progress: $($_.PercentComplete) | Database: $($_.Database) | BackupFile: $($_.BackupFile)" @Logging
                }

                # ---------------------------------- Check/Increase Restore Count and Send out Teams Updates with Current Progress --------------------------------- #
                #region Resore Count and Teams notification

                $CountRestore = $CountRestore + 1

                if ($CountRestore -eq $($ArgumentList.TeamsSendInterval)) {

                    Write-Host "Count is now $CountRestore, Sending Teams Update..!!!"
                    $CountRestore = 0

                    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

                    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                        New-AdaptiveContainer {

                            New-AdaptiveTextBlock -Text "Database Restore Progress" -Size Large -Wrap -HorizontalAlignment Center -Color Accent
                            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
                            New-AdaptiveTable -DataTable $RestoreStatusResults -HeaderColor Good -Spacing Default -HeaderHorizontalAlignment Center -Size Small -Wrap Stretch


                        }
                    }

                }

                #endregion

                # -------------------------------------------------------------------------------------------------------------------------------------------------- #

                Start-Sleep -Seconds $RetryInterval

            }
            $timer.Stop()

            if ($timer.Elapsed.TotalSeconds -gt $Timeout) {

                Write-Host "Timeout exceeded"
                Break
            }
        } catch {

            Write-Host "$($_.Exception.Message)"

        }
    }
    function Remove-DaPSDatabaseSnapshot {
        [CmdletBinding()]
        param (
            [string]$SQLInstance,
            [string]$TravellerDatabase

        )

        begin {

            $Snapshots = (Get-DbaDbSnapshot -SqlInstance $SQLInstance -Database $TravellerDatabase -Verbose).Name
        }

        process {

            if (!$Snapshots) {
                $SnapshotCheckResult = "No SnapShot(s) present for the $TravellerDatabase Database"

            }
            else {
                Remove-DbaDbSnapshot -SqlInstance $SQLInstance -Database $TravellerDatabase -Verbose
                $SnapshotCheckResult = "SnapShot(s) $Snapshots removed from the $TravellerDatabase Database"

            }

        }

        end {
            $SnapshotCheckResult
        }
    }
    function Resize-DaPSTravellerLogs {
        param (
            [string]$SQLInstance ,
            [string]$TravellerDatabase,
            $Logging

        )

        $LogFiles = (Get-DbaDbFile -SqlInstance $SQLInstance -Database $TravellerDatabase | Where-Object {$_.typedescription -eq 'LOG'}).LogicalName

        foreach ($Log in $LogFiles) {

            Invoke-DbaQuery -SqlInstance $SQLInstance -Query "DBCC SHRINKFILE (N'$Log', 1)" -Database $TravellerDatabase
            Write-DaPSLogEvent  "LogFile [$Log] - cleared" @Logging
        }

    }
    function Get-DaPSSnapshotStatus {
        param (
            [string]$SQLInstance,
            [string]$Server,
            $Logging

        )
            $StatusCheck              = @"

                SELECT TOP (1) [agent_id] ,[time] ,[comments]
                FROM [distribution].[dbo].[MSsnapshot_history]
                order by time desc

"@

            do {

                Start-Sleep -s 120

                $CurrentStatus = Invoke-DbaQuery -SqlInstance $SQLInstance -Database distribution -Query $StatusCheck -Verbose # -As PSObject
                Write-DaPSLogEvent "Status - $($CurrentStatus.Comments)" @Logging

            } while ($CurrentStatus.Comments -notlike '*100%]*')

    }
    function Get-DaPSCurrentReplicationSetup {
        [CmdletBinding()]
        param (
            [Parameter(
                ValueFromPipeline = $true,
                ValueFromPipelineByPropertyName = $true,
                Position = 0
            )]
            [string]$SQLInstance
        )
        begin {

        }
        process {

            $GetCurrentReplicationSetup = "exec sp_replmonitorhelppublication @publisher ='$SQLInstance', @publication_type =  0"
            $ReplicationDetails = Invoke-DbaQuery -SqlInstance $SQLInstance -Database distribution -Query $GetCurrentReplicationSetup -As PSObject |
            Select-Object publisher_db, publication #, subscriber
        }
        end {
            Return $ReplicationDetails
        }
    }
    function Reset-DaPSAccountPermissions {
        param (
            [string]$SQLInstance,
            [string]$Database,
            $UserAccountsDBO,
            $UserAccountsReadAccess,
            $UserAccountsWriteAccess,
            $UserAccountsExecutePermissions,
            $UserAccountsBulkAdmin,
            $Logging
        )
        $AccountsApplied = @()
        #   Add the Users and grant permissions
        if (-not [string]::IsNullOrEmpty($UserAccountsDBO)) {
            foreach ($User in $UserAccountsDBO) {

                New-DbaDbUser -SqlInstance $SQLInstance -Database $Database -Login $User  -Username $User | Out-Null
                Add-DbaDbRoleMember -SqlInstance $SQLInstance -Database $Database -Role db_owner -User $User -Confirm:$false | Out-Null
                Write-DaPSLogEvent "$Database - Account Added with DBO permissions - $User" @Logging
                $Account = "Account Added with DBO permissions - $User"
                $AccountsApplied += $Account
            }
        }

        if (-not [string]::IsNullOrEmpty($UserAccountsReadAccess)) {
            foreach ($User in $UserAccountsReadAccess) {

                New-DbaDbUser -SqlInstance $SQLInstance -Database $Database -Login $User  -Username $User | Out-Null
                Add-DbaDbRoleMember -SqlInstance $SQLInstance -Database $Database -Role db_datareader  -User $User -Confirm:$false | Out-Null
                Write-DaPSLogEvent "$Database - Account Added with READ permissions - $User" @Logging
                $Account = "Account Added with READ permissions - $User"
                $AccountsApplied += $Account
            }
        }

        if (-not [string]::IsNullOrEmpty($UserAccountsWriteAccess)){
            foreach ($User in $UserAccountsWriteAccess) {

                New-DbaDbUser -SqlInstance $SQLInstance -Database $Database -Login $User  -Username $User | Out-Null
                Add-DbaDbRoleMember -SqlInstance $SQLInstance -Database $Database -Role db_datawriter  -User $User -Confirm:$false | Out-Null
                Write-DaPSLogEvent "$Database - Account Added with Write permissions - $User" @Logging
                $Account = "Account Added with Write permissions - $User"
                $AccountsApplied += $Account
            }
        }

        if (-not [string]::IsNullOrEmpty($UserAccountsExecutePermissions)) {
            $UserAccountsExecutePermissions | ForEach-Object {
                New-DbaDbUser -SqlInstance $SQLInstance -Database $Database -Login $_  -Username $_  | Out-Null
                #Add-DbaDbRoleMember -SqlInstance $SQLInstance -Database $Database -Role public  -User $User -Confirm:$false
                Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Database -Query "Grant Exec to $_"  | Out-Null
                Write-DaPSLogEvent "$Database - Account Added with Execute permissions - $_" @Logging
                $Account = "Account Added with Execute permissions - $_"
                $AccountsApplied += $Account
            }
        }

        if (-not [string]::IsNullOrEmpty($UserAccountsBulkAdmin)) {
            foreach ($User in $UserAccountsBulkAdmin) {

                Add-DbaServerRoleMember -SqlInstance $SQLInstance -ServerRole 'bulkadmin' -Login $User -Confirm:$false | Out-Null

                Write-DaPSLogEvent "Account Added with BULKADMIN permissions - $User" @Logging
                $Account = "Account Added with BULKADMIN permissions - $User"
                $AccountsApplied += $Account
            }
        }

        $AccountsApplied
    }
    function Remove-DaPSFusionILTView {
        [CmdletBinding()]
        param (
            [string]$SQLInstance,
            [string]$ILTDatabase,
            [string]$ILTView

        )

        begin {
            $VwGetFundingForDiscountAll = Find-DbaView -SqlInstance $SQLInstance -Pattern $ILTView -Database $ILTDatabase

        }

        process {
            if ($VwGetFundingForDiscountAll.Name -eq $ILTView) {

                Invoke-DbaQuery -SqlInstance $SQLInstance -Query "DROP VIEW [dbo].[$ILTView]" -Database $ILTDatabase -Verbose
                $ILTCheckResult = "View [$ILTView] was removed from the $ILTDatabase database"
            }
            else {
                $ILTCheckResult = "View [$ILTView] not in the  $ILTDatabase database"
            }
        }

        end {
            $ILTCheckResult
        }
    }
    function Get-DaPSReplicationStatus {
        param (
            [string]$Server,
            [string]$SQLInstance,
            [string]$TravellerDatabase,
            [string]$PublicationName,
            [string]$ILTDatabase,
            $Logging
        )

        $GetPendingCommands = @"

            EXEC  sp_replmonitorsubscriptionpendingcmds
                  @publisher    = '$Server'
                , @publisher_db = '$TravellerDatabase'
                , @publication  = '$PublicationName'
                , @subscriber   = '$Server'
                , @subscriber_db= '$ILTDatabase'
                , @subscription_type = '0'

"@

        $ReplicationStatus = @"

            SELECT TOP (1) [agent_id]
            ,[runstatus]
            ,[time]
            ,DATEPART(MINUTE, DATEADD(ss, duration, '19000101')) as minutes
            ,[comments]
            FROM [distribution].[dbo].[MSdistribution_history] MSDH
            join [distribution].[dbo].[MSdistribution_agents] MSDA on MSDH.agent_id = MSDA.id
            where MSDA.publisher_db = '$TravellerDatabase'
            order by time desc
"@

        $CommandThreshold = 100

        # Get the count of current pending commands in the distribution database
        do {

            $PendingCommands = Invoke-DbaQuery -SqlInstance $SQLInstance -Database distribution -Query $GetPendingCommands -Verbose

            $CurrentStatus = Invoke-DbaQuery -SqlInstance $SQLInstance -Database distribution -Query $ReplicationStatus -Verbose # -As PSObject
            Write-DaPSLogEvent "$($CurrentStatus.minutes)mins - $($CurrentStatus.comments)" @Logging

            if ($($CurrentStatus.comments) -match 'ANSI_PADDING') {

                $QuerySnapshotFolder = 'select working_directory from msdb.dbo.MSdistpublishers'
                $SnapshotFolder = Invoke-DbaQuery -SqlInstance $SQLInstance -Query $QuerySnapshotFolder

                $FileList       = Get-ChildItem -Path $SnapshotFolder.working_directory -Include *.sch -File -Recurse
                $FilesToUpdate  = Select-String -Path $FileList.FULLNAME -Pattern 'SET ANSI_PADDING OFF'
                if ($FilesToUpdate.count -gt 0) {

                    (Get-Content -Path $FilesToUpdate.path) -replace 'SET ANSI_PADDING OFF', 'SET ANSI_PADDING ON' | Set-Content -Path $FilesToUpdate.path
                }
                Write-DaPSLogEvent "ANSI-PADDING error. $($FilesToUpdate.count) files updated" @Logging
            }

            if ($PendingCommands.pendingcmdcount -gt $CommandThreshold) {Start-Sleep 120}

        } while ($PendingCommands.pendingcmdcount -gt $CommandThreshold)

    }

    $ThreadJobFunctions = {

        # -------------------------------------------------------------------------------------------------------------------------------------------------- #
        #                                                         FUNCTIONS PASSED INTO THE THREADJOB                                                        #
        # -------------------------------------------------------------------------------------------------------------------------------------------------- #
        #region Thread Job Functions

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

                        Write-Host "$(get-date -format "dd/MM/yyy HH:mm:ss") : $LogEvent" -ForegroundColor Yellow

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

        #endregion

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                        CHECK / CREATE LOGGROUP AND LOGSTREAM                                                       #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #

    New-DaPSLogGroupLogStream $Logging.LogGroupName $Logging.LogStreamName $Logging.ProfileName

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                           CHECK AND CLEAN UP THREAD JOBS                                                           #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Thread Jobs

    $RefreshJobs = Get-Job | Where-Object {$_.Name -match 'Refresh'} | Remove-Job
    if (!($RefreshJobs)) {

        Write-DaPSLogEvent "No Refresh Thread Jobs Running" @Logging

    }else {

        $RefreshJobs | ForEach-Object {
            Write-DaPSLogEvent "[$($_.Name)] Job Still Available. Removing...!!!" @Logging
            $_ | Remove-Job
        }
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                        CREATE TRAVELLER REPLICATION SCRIPTS                                                        #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Replication Scripts

    if (!(Test-Path $FolderReplicationScriptsSDLC)) {

        New-Item -ItemType Directory -Path $FolderReplicationScriptsSDLC -Force
        Write-DaPSLogEvent "Created Replication Scripts Folder - $FolderReplicationScriptsSDLC" @Logging

    }

    start-sleep -Seconds 2

    $ReplicationScriptsLive     = @($TravellerReplicationILTLive, $TravellerReplicationSupplierExtrasLive, $SILTReplicationILTLive, $SILTReplicationAvailabilityCacheLive)
    $SearchTravellerPublication = "@publication = N'pubFusionILTCache'"
    $ReplacePublication         = "@publication = N'$($Env.TravellerPublicationILT)'"

    $ReplicationScriptsLive | ForEach-Object {

        switch ($_) {

            $TravellerReplicationILTLive {

                $TravellerRepILTLive = Get-Content $_
                $TravellerRepILTLive | ForEach-Object {
                    $_ -replace 'TR4_LIVE',$Database -replace $SearchTravellerPublication , $ReplacePublication -replace 'VRUK-A-SILCLUS', $Env.Server -replace 'FusionILTCacheSearch',$($Env.ILTDatabaseSILT)
                } | Set-Content $TravellerReplicationILTSDLC
                Write-DaPSLogEvent "Created Replication Script - $TravellerReplicationILTSDLC" @Logging

            }
            $TravellerReplicationSupplierExtrasLive {

                $TravellerRepSupplierExtrasLive = Get-Content $_
                $TravellerRepSupplierExtrasLive | ForEach-Object {
                    $_ -replace 'TR4_LIVE',$Database -replace 'pubFusionILTCache - Supplier Extras',$($Env.TravellerPublicationILTSupplierExtras) -replace 'VRUK-A-SILCLUS', $Env.Server -replace 'FusionILTCacheSearch',$($Env.ILTDatabaseSILT)
                } | Set-Content $TravellerReplicationSupplierExtrasSDLC
                Write-DaPSLogEvent "Created Replication Script - $TravellerReplicationSupplierExtrasSDLC" @Logging

            }
            $SILTReplicationILTLive {

                $SILTRepILTLive = Get-Content $_
                $SILTRepILTLive | ForEach-Object {
                    $_ -replace '\[FusionILTCacheSearch\]', "[$($Env.ILTDatabaseSILT)]" -replace "N'FusionILTCacheSearch'", "N'$($Env.ILTDatabaseSILT)'" -replace "N'pubFusionILTCacheSearch'", "N'$($Env.PublicationNameSILT)'"
                } | Set-Content $SILTReplicationILTSDLC
                Write-DaPSLogEvent "Created Replication Script - $SILTReplicationILTSDLC" @Logging

            }
            $SILTReplicationAvailabilityCacheLive {

                $SILTRepAvailabilityCacheLive = Get-Content $_
                $SILTRepAvailabilityCacheLive | ForEach-Object {
                    $_ -replace 'FusionILTCacheSearch',$($Env.ILTDatabaseSILT) -replace 'FusionILTAvailabilityCache',$($Env.SILTPublicationAvailabilityCache)
                } | Set-Content $SILTReplicationAvailabilityCacheSDLC
                Write-DaPSLogEvent "Created Replication Script - $SILTReplicationAvailabilityCacheSDLC" @Logging

            }
        }
    }
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                 TEAMS NOTIFICATION - SUMMARY OF THE REFRESH PROCESS                                                #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Teams Notification

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Environment Refresh Started" -Size ExtraLarge -Wrap -HorizontalAlignment Center -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text "$Database" -Subtle -HorizontalAlignment Center -Color Good -Size Large -Spacing None

        }
    } -Action {
        New-AdaptiveAction -Title "Refresh Process" -Body   {
            New-AdaptiveTextBlock -Text "Environment Refresh Process" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {

                New-AdaptiveFact -Title '01.' -Value "Check/Remove Replication"
                New-AdaptiveFact -Title '02.' -Value "Check/Remove Database Snapshots"
                New-AdaptiveFact -Title '03.' -Value "Disable Owner Payment job"
                New-AdaptiveFact -Title '04.' -Value "Disable CDC"
                New-AdaptiveFact -Title '05.' -Value "Refresh $($Env.Database) database"
                New-AdaptiveFact -Title '06.' -Value "Enable Change Tracking"
                New-AdaptiveFact -Title '07.' -Value "Drop/Add User Accounts"
                New-AdaptiveFact -Title '08.' -Value "Enable CDC"
                New-AdaptiveFact -Title '09.' -Value "Update $($Env.Database) Traveller Application Settings"
                New-AdaptiveFact -Title '10.' -Value "Refresh $($Env.ILTDatabase) database"
                New-AdaptiveFact -Title '11.' -Value "Create Publication $($Env.PublicationName)"
                New-AdaptiveFact -Title '12.' -Value "Setup Replication"
                New-AdaptiveFact -Title '13.' -Value "Apply Indexes to the $($Env.ILTDatabase) database"
                New-AdaptiveFact -Title '14.' -Value "Check Traveller Discount Engine has loaded"


            } -Separator Medium
        }

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              START ENVIRONMENT REFRESH                                                             #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Environment Refresh Start

    Start-MyTimer -Name $TimerEnvironmentRefresh
    Write-DaPSLogEvent "*************$Database Environment Refresh Started*************"  @Logging
    
    #endregion
    
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                            CHECK AND REMOVE REPLICATION                                                            #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Replication Clean Up

    $CurrentReplication = Get-DaPSCurrentReplicationSetup -SQLInstance $Env.Server
    $Publications = $CurrentReplication | Where-Object {$_.publisher_db -eq $Database -or $_.publisher_db -eq  $($Env.ILTDatabaseSILT)}

    while ($Publications) {

        foreach ($Publication in $Publications) {

            $DropReplication    = @"

                EXEC sp_dropsubscription
                        @publication = N'$($Publication.publication)',
                        @article = N'all',
                        @subscriber = N'all';

                EXEC sp_droppublication @publication = N'$($Publication.publication)';

"@

        Invoke-DbaQuery -SqlInstance $SQLInstance -Database $($Publication.publisher_db) -Query $DropReplication -Verbose

        Start-Sleep 2

        $RemovalCheck = Get-DaPSCurrentReplicationSetup -SQLInstance $Env.Server | Where-Object {$_.publication -eq $($Publication.publication)}

        if ($RemovalCheck) {

            Write-DaPSLogEvent "PUBLICATION NOT REMOVED - Publication: [$($Publication.publication)] | Database: [$($Publication.publisher_db)]" @Logging

        }else {

            Write-DaPSLogEvent "PUBLICATION REMOVED - Publication: [$($Publication.publication)] | Database: [$($Publication.publisher_db)]" @Logging
        }


        }

        Start-Sleep 3

        $CurrentReplication = Get-DaPSCurrentReplicationSetup -SQLInstance $Env.Server
        $Publications = $CurrentReplication | Where-Object {$_.publisher_db -eq $Database -or $_.publisher_db -eq  $($Env.ILTDatabaseSILT)}

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                         CHECK AND REMOVE DATABASE SNAPSHOTS                                                        #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Database Snapshots

    $SnapshotCheckResults = Remove-DaPSDatabaseSnapshot  $SQLInstance $Database
    Write-DaPSLogEvent $SnapshotCheckResults @Logging
    
    #endregion
    
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              DISABLE OWNER PAYMENT JOB                                                             #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Disable Owner Payment Job

    if ($Env.OwnerPaymentJob) {
        Set-DbaAgentJob -SqlInstance $SQLInstance -Job $($Env.OwnerPaymentJob) -Disabled
        Write-DaPSLogEvent "[$($Env.OwnerPaymentJob)] - SQL job has been disabled" @Logging
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                     DISABLE CDC                                                                    #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Disable CDC

    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Database -Query 'EXEC sys.sp_cdc_disable_db' -Verbose
    Write-DaPSLogEvent 'Disabled/Removed CDC jobs' @Logging
    
    #endregion
    
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                          KILL CONNECTIONS TO ALL DATABASES                                                         #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Kill Connections

    $($Database), $($EnvILT.Database), $($EnvILTSILT.Database) | ForEach-Object {

        # Write-DaLogEvent "******************Checking $($_) Database Activity******************" @Logging

        $SQLActivity = Invoke-DbaQuery -SqlInstance $SQLInstance -Database Master -Query 'exec sp_who2' -As PSObject
        $ActiveSPIDs =  $SQLActivity | Where-Object {$_.DBName -eq $_ -and $_.SPID -gt 50 }

        # Note: SPID 50 and above are user transactions. 50 and below are system activiy abd can not be killed.

        if ([string]::IsNullOrEmpty($ActiveSPIDs)) {

            Write-DaPSLogEvent "[SQL Activity Check] - No SQL activity for the $_ database" @Logging
        }
        else {

            $ActiveSPIDs | ForEach-Object {

                Invoke-DbaQuery -SqlInstance $SQLInstance -Database Master -Query "kill $($_.SPID)"
                Write-DaPSLogEvent "Killed SPID [$($_.SPID.trim())] - Login:[$($_.Login)] - Command: [$($_.Command)] - Program: [$($_.ProgramName.trim())]" @Logging
            }

        }

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                               DISABLE SQL/AD ACCOUNTS                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Disable SQL/AD Accounts

    $AccountsAccess = ($AllUserAccounts | Select-Object -Unique User | Where-Object {$_ -notmatch 'VRUKL'} ).User

    $AccountsAccess | ForEach-Object {

        Set-DbaLogin -SqlInstance $SQLInstance -Login $_ -DenyLogin
        Set-DbaLogin -SqlInstance $SQLInstance -Login $_ -Disable
        Write-DaPSLogEvent  "SQL Account Disabled - [$_]" @Logging
    }

    Write-DaPSLogEvent  "All SQL Accounts Disabled" @Logging

    Get-DbaLogin -SqlInstance $SQLInstance | Where-Object {$_.Name -in $AccountsAccess}  | Select-Object Name, Hasaccess, IsDisabled

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                          SETUP BACKUP FILES - LIVE\MASKED                                                          #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Backup Files

    switch ($Env.BackupType) {
        Masked {

            $Backups    = $Params.'BackUpFolder-Masked-Traveller'
            $MaskedBackupFile = Get-ChildItem -Path $($Params.'BackUpFolder-Masked-Traveller') | Where-Object {$_.Name -match 'bak'} |
                                Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1

            Write-DaPSLogEvent "Backup Type Set: $($Env.BackupType)" @Logging
        }
        Live {
            Write-DaPSLogEvent "Backup Type Set: $($Env.BackupType)" @Logging


            $Backups = @('\\backups_fsx_TRAV\SQL_Backups_Traveller\TRAVELLERSQLCL\TR4_LIVE',
                        '\\backups_fsx_TRAV\SQL_Backups_Traveller\TRAVELLERSQLCL_DIFF\TR4_LIVE',
                        '\\backups_fsx_TRAV\SQL_Backups_Traveller\TRAVELLERSQLCL_TLOG\TR4_LIVE')
        }
        default {

            Write-DaPSLogEvent '[INFO]: No Backup Type set. Using the Masked Backup to restore' @Logging
            $Backups  = $Params.'BackUpFolder-Masked-Traveller'
            $MaskedBackupFile = Get-ChildItem -Path $($Params.'BackUpFolder-Masked-Traveller') | Where-Object {$_.Name -match 'bak'} |
                                Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        }
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                               DATABASE REFRESH - SILT                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Database Refresh SILT

    $ArgsListSILT = @($EnvILTSILT, $Params, $URI, $Logging)

    $RefreshSILT = {

        Param ( $EnvILTSILT,$Params,$URI, $Logging )

        try {

            # ------------------------------------------------- Teams Notification - Starting Database Refresh ------------------------------------------------- #
            #region Teams Notification - Refresh Started

            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    New-AdaptiveTextBlock -Text "Database Refresh Started" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
                    New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
                    New-AdaptiveTextBlock -Text $($EnvILTSILT.Database) -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good

                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title "SQL Instance" -Value "$($EnvILTSILT.SQLInstance)"

                    }  -Separator Medium

                }
            }

            #endregion

            Write-DaPSLogEvent "[$($EnvILTSILT.Database)] Database Refresh Started...!!!!" @Logging | Out-Null

            $RestoreParameters = @{

                SqlInstance                 = $EnvILTSILT.SQLInstance
                DatabaseName                = $EnvILTSILT.Database
                DestinationDataDirectory    = $EnvILTSILT.DataDirectory
                DestinationLogDirectory     = $EnvILTSILT.LOGDirectory
                DestinationFileSuffix       = $EnvILTSILT.Suffix
                WithReplace                 = $true
                ErrorAction                 = 'Stop'

            }

            $RestoreSummarySILT = $($Params.'Folder-ILT-Backup') | Restore-DbaDatabase @RestoreParameters -Verbose
            $RestoreSummarySILT

            Write-DaPSLogEvent "[$($EnvILTSILT.Database)] Database Refresh Completed. Runtime: $($RestoreSummarySILT.DatabaseRestoreTime.ToString("hh\:mm\:ss"))" @Logging | Out-Null

            # ----------------------------------------------------- Teams Notification - Refresh Completed ----------------------------------------------------- #
            #region Teams Notification - Refresh Completed

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    New-AdaptiveTextBlock -Text "Database Refresh Completed" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
                    New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
                    New-AdaptiveTextBlock -Text $($EnvILTSILT.Database) -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good

                }
            } -Action {
                New-AdaptiveAction -Title "Refresh Summary" -Body   {
                    New-AdaptiveTextBlock -Text "Database Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Duration' -Value $($RestoreSummarySILT.DatabaseRestoreTime.ToString("hh\:mm\:ss"))
                        New-AdaptiveFact -Title 'Date' -Value "$((Get-Date).GetDateTimeFormats()[12])"
                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($RestoreSummarySILT.SQLInstance)"
                        New-AdaptiveFact -Title "Server" -Value "$($Env.Server)"

                    } -Separator Medium
                    New-AdaptiveTextBlock -Text "Refresh Parameters" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Backup' -Value $(($RestoreSummarySILT.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Backup Size(GB)' -Value $([math]::Round($($RestoreSummarySILT.BackupSize.Gigabyte),2))
                        New-AdaptiveFact -Title 'Data Directory' -Value "$($EnvILTSILT.DataDirectory)"
                        New-AdaptiveFact -Title 'Log Directory' -Value "$($EnvILTSILT.LOGDirectory)"
                        New-AdaptiveFact -Title 'FileSuffix' -Value "$($EnvILTSILT.Suffix)"


                    } -Separator Medium
                }
                New-AdaptiveAction -Title "Script" -Body {

                    New-AdaptiveTextBlock -Text "Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveLineBreak
                    New-AdaptiveTextBlock  -Text $($RestoreSummarySILT.Script) -Wrap

                }
            }

            #endregion


        }
        catch {

            # ----------------------------------------------------- Teams Notification - Refresh Failed ----------------------------------------------------- #
            #region Teams Notification - Refresh Failed

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    New-AdaptiveTextBlock -Text "Environment Refresh" -Size Large -Wrap -HorizontalAlignment Center -Color Accent
                    New-AdaptiveTextBlock -Text "Database Refresh Failed" -HorizontalAlignment Center -Spacing None -Size ExtraLarge -Color Attention
                    New-AdaptiveFactSet {
                        New-AdaptiveFact -Title " " -Value " "
                        New-AdaptiveFact -Title "Database" -Value $EnvILTSILT.Database
                        New-AdaptiveFact -Title "Instance" -Value "$($EnvILTSILT.SQLInstance)"
                        New-AdaptiveFact -Title "Error" -Value $_

                    }  -Separator Medium


                }
            }

            #endregion

            Write-DaPSLogEvent "[$($EnvILTSILT.Database)] Database restore failed...!!!!!" @Logging | Out-Null
            Write-DaPSLogEvent "$_" @Logging | Out-Null

            $RestoreParameters = @{

                SqlInstance                 = $EnvILTSILT.SQLInstance
                DatabaseName                = $EnvILTSILT.Database
                DestinationDataDirectory    = $EnvILTSILT.DataDirectory
                DestinationLogDirectory     = $EnvILTSILT.LOGDirectory
                DestinationFileSuffix       = $EnvILTSILT.Suffix
                WithReplace                 = $true
                ErrorAction                 = 'Stop'

            }

            $RestoreSummarySILT = $($Params.'Folder-ILT-Backup') | Restore-DbaDatabase @RestoreParameters | Out-Null
            $RestoreSummarySILT

            # ----------------------------------------------------- Teams Notification - Refresh Completed ----------------------------------------------------- #
            #region Teams Notification - Refresh Completed

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    New-AdaptiveTextBlock -Text "Environment Refresh" -Size Large -Wrap -HorizontalAlignment Center -Color Accent
                    New-AdaptiveTextBlock -Text "Database Refresh Completed" -Subtle -HorizontalAlignment Center -Spacing None -Size Large
                    New-AdaptiveTextBlock -Text $($RestoreSummaryILT.Database) -Subtle -HorizontalAlignment Center -Color Good -Size Large -Spacing None

                }
            } -Action {
                New-AdaptiveAction -Title "Refresh Summary" -Body   {
                    New-AdaptiveTextBlock -Text "Database Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Duration' -Value $($RestoreSummaryILT.DatabaseRestoreTime.ToString("hh\:mm\:ss"))
                        New-AdaptiveFact -Title 'Date' -Value "$((Get-Date).GetDateTimeFormats()[12])"
                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($RestoreSummaryILT.SQLInstance)"
                        New-AdaptiveFact -Title "Server" -Value "$($Env.Server)"

                    } -Separator Medium
                    New-AdaptiveTextBlock -Text "Refresh Parameters" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Backup' -Value $(($SummaryILT.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Backup Size(MB)' -Value $($SummaryILT.BackupSize.Megabyte)
                        New-AdaptiveFact -Title 'Data Directory' -Value "$($Env.ILTDataDirectory)"
                        New-AdaptiveFact -Title 'Log Directory' -Value "$($Env.ILTLogDirectory)"
                        New-AdaptiveFact -Title 'FileSuffix' -Value "$($Env.FileSuffix)"


                    } -Separator Medium
                }
                New-AdaptiveAction -Title "Script" -Body {

                    New-AdaptiveTextBlock -Text "Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveLineBreak
                    New-AdaptiveTextBlock  -Text $($SummaryILT.Script)

                }
            }

            #endregion

        }

    }

    $paramsThreadJobSILT = @{

        Name                    = "Refresh#$($EnvILTSILT.Database)"
        ScriptBlock             = $RefreshSILT
        ArgumentList            = $ArgsListSILT
        InitializationScript    = $ThreadJobFunctions

    }

    Start-ThreadJob @paramsThreadJobSILT # -StreamingHost $Host

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                            DATABASE REFRESH - TRAVELLER                                                            #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Database Refresh Traveller

    $ArgsListTraveller = @( $Backups, $Env,$SQLInstance,$Database,$URI, $Logging)

    $RefreshTraveller = {

        Param ($Backups, $Env,$SQLInstance,$Database,$URI, $Logging)

        $FileStructure = @{

            'Tr@veller_Data'    =	$Env.Traveller_Data
            'Tr@veller_Log'     =	$Env.Traveller_Log
            'Traveller_Data2'   =	$Env.Traveller_Data2
            'Traveller_AddData'	=	$Env.Traveller_AddData
            'Tr@veller_Log2'    =	$Env.Traveller_Log2
            'Traveller_Log3'    =	$Env.Traveller_Log3

        }

        try {

            # ------------------------------------------------- Teams Notification - Starting Database Refresh ------------------------------------------------- #
            #region Teams Notification - Refresh Started

            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    # New-AdaptiveTextBlock -Text "Environment Refresh" -Size Large -Wrap -HorizontalAlignment Center -Color Accent
                    New-AdaptiveTextBlock -Text "Database Refresh Started" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
                    New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
                    New-AdaptiveTextBlock -Text $Database -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title "SQL Instance" -Value "$($Env.SQLInstance)"

                    }  -Separator Medium

                }
            }

            #endregion

            $RestoreParameters = @{

                SqlInstance                 = $SQLInstance
                DatabaseName                = $Database
                FileMapping                 = $FileStructure
                WithReplace                 = $true
                ErrorAction                 = 'Stop'
                ErrorVariable               = 'RefreshWarning'

            }

            Write-DaPSLogEvent "[$Database] Database Refresh Started...!!!!" @Logging | Out-Null

            $RestoreSummaryTraveller = $Backups | Restore-DbaDatabase @RestoreParameters
            $RestoreSummaryTraveller

            switch ($Env.BackupType) {
                Masked {

                    Write-DaPSLogEvent "[$Database)] Database Refresh Completed. Runtime: $($RestoreSummaryTraveller.DatabaseRestoreTime.ToString("hh\:mm\:ss"))" @Logging | Out-Null

                }
                Live {

                    $FULLBackup = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'FULL'} | Select-Object -Last 1
                    $DIFFBackup = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'DIFF'} | Select-Object -Last 1
                    $TRNBackups = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'TRN'}
                    $LastBackup = $RestoreSummaryTraveller | Select-Object -Last 1

                    Write-DaPSLogEvent "[$Database)] Database Refresh Completed. Runtime: $($LastBackup.DatabaseRestoreTime)" @Logging | Out-Null
                }
            }

            # ----------------------------------------------------- Teams Notification - Refresh Completed ----------------------------------------------------- #
            #region Teams Notification - Refresh Completed

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    New-AdaptiveTextBlock -Text "Database Refresh Completed" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
                    New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
                    switch ($Env.BackupType) {
                        Masked {

                            New-AdaptiveTextBlock -Text $($RestoreSummaryTraveller.Database) -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good
                        }
                        Live {

                            $FULLBackup = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'FULL'} | Select-Object -Last 1

                            New-AdaptiveTextBlock -Text $($FULLBackup.Database) -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good
                        }
                    }

                }
            } -Action {
                switch ($Env.BackupType) {
                    Masked {
                        New-AdaptiveAction -Title "Refresh Summary" -Body   {
                            New-AdaptiveTextBlock -Text "Database Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveFactSet {

                                New-AdaptiveFact -Title 'Duration' -Value $($RestoreSummaryTraveller.DatabaseRestoreTime.ToString("hh\:mm\:ss"))
                                New-AdaptiveFact -Title 'Date' -Value "$((Get-Date).GetDateTimeFormats()[12])"
                                New-AdaptiveFact -Title 'SQL Instance' -Value "$($RestoreSummaryTraveller.SQLInstance)"
                                New-AdaptiveFact -Title "Server" -Value "$($env:COMPUTERNAME)"

                            } -Separator Medium
                            New-AdaptiveTextBlock -Text "Refresh Parameters" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveFactSet {

                                New-AdaptiveFact -Title 'Backup' -Value $(($RestoreSummaryTraveller.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                New-AdaptiveFact -Title 'Backup Size(GB)' -Value $([math]::Round($($RestoreSummaryTraveller.BackupSize.Gigabyte),2))


                            } -Separator Medium
                        }
                        New-AdaptiveAction -Title "Script" -Body {

                            New-AdaptiveTextBlock -Text "Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveLineBreak
                            New-AdaptiveTextBlock  -Text $($RestoreSummaryTraveller.Script) -Wrap

                        }
                    }
                    Live {

                        $FULLBackup = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'FULL'} | Select-Object -Last 1
                        $DIFFBackup = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'DIFF'} | Select-Object -Last 1
                        $TRNBackups = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'TRN'}
                        $LastBackup = $RestoreSummaryTraveller | Select-Object -Last 1

                        New-AdaptiveAction -Title "Refresh Summary" -Body   {
                            New-AdaptiveTextBlock -Text "Database Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveFactSet {

                                New-AdaptiveFact -Title 'Duration' -Value "$($LastBackup.DatabaseRestoreTime)"
                                New-AdaptiveFact -Title 'Date' -Value "$((Get-Date).GetDateTimeFormats()[12])"
                                New-AdaptiveFact -Title 'SQL Instance' -Value "$($FULLBackup.SQLInstance)"
                                New-AdaptiveFact -Title "Server" -Value "$($FULLBackup.ComputerName)"

                            } -Separator Medium

                        }
                        New-AdaptiveAction -Title "Backup Files" -Body {

                            New-AdaptiveTextBlock -Text "Full Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveFactSet {

                                New-AdaptiveFact -Title 'Full Backup' -Value $(($FULLBackup.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                New-AdaptiveFact -Title 'Size' -Value "$($FULLBackup.BackupSize)"
                                New-AdaptiveFact -Title 'Size Compressed' -Value "$($FULLBackup.CompressedBackupSize)"
                                New-AdaptiveFact -Title 'Restore Time' -Value "$($FULLBackup.FileRestoreTime)"


                            } -Separator Medium

                            New-AdaptiveTextBlock -Text "DIFF Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveFactSet {

                                New-AdaptiveFact -Title 'DIFF Backup' -Value $(($DIFFBackup.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                New-AdaptiveFact -Title 'Size' -Value "$($DIFFBackup.BackupSize)"
                                New-AdaptiveFact -Title 'Size Compressed' -Value "$($DIFFBackup.CompressedBackupSize)"
                                New-AdaptiveFact -Title 'Restore Time' -Value "$($DIFFBackup.FileRestoreTime)"


                            } -Separator Medium

                            New-AdaptiveTextBlock -Text "Log Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveFactSet {

                                New-AdaptiveFact -Title 'Last Log File' -Value $((($TRNBackups | Select-Object -Last 1).BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                New-AdaptiveFact -Title 'Log Files Count' -Value $TRNBackups.Count
                                New-AdaptiveFact -Title 'Logs Restore Time' -Value $([timespan]::fromseconds($(($TRNBackups.FileRestoreTime | Measure-Object -Property TotalSeconds -Sum).Sum)).ToString())

                            } -Separator Medium

                        }
                        New-AdaptiveAction -Title "Scripts" -Body {

                            New-AdaptiveTextBlock -Text "FULL Restore" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveLineBreak
                            New-AdaptiveTextBlock  -Text $($FULLBackup.Script) -Wrap

                            New-AdaptiveTextBlock -Text "DIFF Restore" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                            New-AdaptiveLineBreak
                            New-AdaptiveTextBlock  -Text $($DIFFBackup.Script) -Wrap

                        }
                    }
                }
            }

            #endregion

        }
        catch {

            # ----------------------------------------------------- Teams Notification - Refresh Failed ----------------------------------------------------- #
            #region Teams Notification - Refresh Failed

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    New-AdaptiveTextBlock -Text "Environment Refresh" -Size Large -Wrap -HorizontalAlignment Center -Color Accent
                    New-AdaptiveTextBlock -Text "Database Refresh Failed. Retrying..!!" -HorizontalAlignment Center -Spacing None -Size ExtraLarge -Color Attention
                    New-AdaptiveFactSet {
                        New-AdaptiveFact -Title " " -Value " "
                        New-AdaptiveFact -Title "Database" -Value $Database
                        New-AdaptiveFact -Title "Instance" -Value "$($Env.SQLInstance)"
                        New-AdaptiveFact -Title "Error" -Value $_

                    }  -Separator Medium

                }
            }

            #endregion

            Write-DaLogEvent $logfile "Database restore failed. Retrying...!!!!!" | Out-Null

            $RestoreParameters = @{

                SqlInstance                 = $SQLInstance
                DatabaseName                = $Database
                FileMapping                 = $FileStructure
                WithReplace                 = $true
                ErrorAction                 = 'Stop'

            }

            $RestoreSummaryTraveller = $Backups | Restore-DbaDatabase @RestoreParameters
            $RestoreSummaryTraveller

            # ----------------------------------------------------- Teams Notification - Refresh Completed ----------------------------------------------------- #
            #region Teams Notification - Refresh Completed

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
                New-AdaptiveContainer {

                    New-AdaptiveTextBlock -Text "Database Refresh Completed" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
                    New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
                    switch ($Env.BackupType) {
                        Masked {

                            New-AdaptiveTextBlock -Text $($RestoreSummaryTraveller.Database) -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good
                        }
                        Live {
                            New-AdaptiveTextBlock -Text $($FULLBackup.Database) -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good
                        }
                    }

                        }
                    } -Action {
                        switch ($Env.BackupType) {
                            Masked {
                                New-AdaptiveAction -Title "Refresh Summary" -Body   {
                                    New-AdaptiveTextBlock -Text "Database Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveFactSet {

                                        New-AdaptiveFact -Title 'Duration' -Value $($RestoreSummaryTraveller.DatabaseRestoreTime.ToString("hh\:mm\:ss"))
                                        New-AdaptiveFact -Title 'Date' -Value "$((Get-Date).GetDateTimeFormats()[12])"
                                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($RestoreSummaryTraveller.SQLInstance)"
                                        New-AdaptiveFact -Title "Server" -Value "$($env:COMPUTERNAME)"

                                    } -Separator Medium
                                    New-AdaptiveTextBlock -Text "Refresh Parameters" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveFactSet {

                                        New-AdaptiveFact -Title 'Backup' -Value $(($RestoreSummaryTraveller.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                        New-AdaptiveFact -Title 'Backup Size(GB)' -Value $([math]::Round($($RestoreSummaryTraveller.BackupSize.Gigabyte),2))


                                    } -Separator Medium
                                }
                                New-AdaptiveAction -Title "Script" -Body {

                                    New-AdaptiveTextBlock -Text "Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveLineBreak
                                    New-AdaptiveTextBlock  -Text $($RestoreSummaryTraveller.Script) -Wrap

                                }
                            }
                            Live {

                                $FULLBackup = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'FULL'} | Select-Object -Last 1
                                $DIFFBackup = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'DIFF'} | Select-Object -Last 1
                                $TRNBackups = $RestoreSummaryTraveller | Where-Object {$_.BackupFile -match 'TRN'}

                                New-AdaptiveAction -Title "Refresh Summary" -Body   {
                                    New-AdaptiveTextBlock -Text "Database Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveFactSet {

                                        New-AdaptiveFact -Title 'Duration' -Value "$($TRNBackup.DatabaseRestoreTime)"
                                        New-AdaptiveFact -Title 'Date' -Value "$((Get-Date).GetDateTimeFormats()[12])"
                                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($FULLBackup.SQLInstance)"
                                        New-AdaptiveFact -Title "Server" -Value "$($FULLBackup.ComputerName)"

                                    } -Separator Medium

                                }
                                New-AdaptiveAction -Title "Backup Files" -Body {

                                    New-AdaptiveTextBlock -Text "Full Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveFactSet {

                                        New-AdaptiveFact -Title 'Full Backup' -Value $(($FULLBackup.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                        New-AdaptiveFact -Title 'Size' -Value "$($FULLBackup.BackupSize)"
                                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($FULLBackup.CompressedBackupSize)"
                                        New-AdaptiveFact -Title 'Restore Time' -Value "$($FULLBackup.FileRestoreTime)"


                                    } -Separator Medium

                                    New-AdaptiveTextBlock -Text "DIFF Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveFactSet {

                                        New-AdaptiveFact -Title 'DIFF Backup' -Value $(($DIFFBackup.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                        New-AdaptiveFact -Title 'Size' -Value "$($DIFFBackup.BackupSize)"
                                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($DIFFBackup.CompressedBackupSize)"
                                        New-AdaptiveFact -Title 'Restore Time' -Value "$($DIFFBackup.FileRestoreTime)"


                                    } -Separator Medium

                                    New-AdaptiveTextBlock -Text "Log Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveFactSet {

                                        New-AdaptiveFact -Title 'Last Log File' -Value $((($TRNBackups | Select-Object -Last 1).BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                                        New-AdaptiveFact -Title 'Log Files Count' -Value $TRNBackups.Count
                                        New-AdaptiveFact -Title 'Logs Restore Time' -Value $([timespan]::fromseconds($(($TRNBackups.FileRestoreTime | Measure-Object -Property TotalSeconds -Sum).Sum)).ToString())

                                    } -Separator Medium

                                }
                                New-AdaptiveAction -Title "Scripts" -Body {

                                    New-AdaptiveTextBlock -Text "FULL Restore" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveLineBreak
                                    New-AdaptiveTextBlock  -Text $($FULLBackup.Script) -Wrap

                                    New-AdaptiveTextBlock -Text "DIFF Restore" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                                    New-AdaptiveLineBreak
                                    New-AdaptiveTextBlock  -Text $($DIFFBackup.Script) -Wrap

                                }
                            }
                        }

                    }

            #endregion
        }

    }

    $paramsThreadJob = @{

        Name                    = "Refresh#$($Env.Database)"
        ScriptBlock             = $RefreshTraveller
        ArgumentList            = $ArgsListTraveller
        InitializationScript    = $ThreadJobFunctions

    }

    Start-ThreadJob @paramsThreadJob  #-StreamingHost $Host

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              MONITOR RESTORE PROGRESS                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Monitor Restore Progress

    Start-Sleep 90

    $RefreshThreadJobCheck = {

        $AllJobs = Get-Job | Where-Object {$_.Name -match 'Refresh'}

        $arrayJobsInProgress = $AllJobs | Where-Object { ($_.State -match 'Running') -or ($_.State -match 'NotStarted') }

        if ($arrayJobsInProgress) {

            return $true
        }
        else {

            Write-Host "All Jobs Completed"
        }

    }

    $argsRefreshWait = [PSCustomObject]@{

        SQLInstance         = $SQLInstance
        RegexBackupFile     = $RegexBackupFile
        RegexDatabaseName   = $RegexDatabaseName
        URI                 = $URI
        TeamsSendInterval   = $TeamsSendInterval
    }

    Wait-DaPSDatabaseRefreshCheck -Condition $RefreshThreadJobCheck -Timeout 20000000 -RetryInterval 150 -ArgumentList $argsRefreshWait -Logging $Logging -Verbose

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                REMOVE ALL TRAVELLER ACCESS FROM NON MASKED DATABASE                                                #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Remove Traveller Access

    if ($Env.BackupType -ne 'Masked') {

        $LoginTableUpdate = "

            UPDATE [TR4_UAT].[dbo].[Login] set Pass = ''
            where Login NOT IN ('JWorthington', 'Bboardwell','Cgauntlett')

        "

        Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Database -Query $LoginTableUpdate -Verbose
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                      SET TRAVELLER DATABASE TO SIMPLE RECOVERY                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Set Traveller Database to Simple Recovery

    Set-DbaDbRecoveryModel -SqlInstance $SQLInstance -Database $Database -RecoveryModel Simple -Confirm:$false
    Write-DaPSLogEvent "$Database set to SIMPLE Recovery" @Logging

    Start-Sleep 5
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                SHRINK SILT LOG FILES                                                               #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Shrink SILT Log Files

    $LogFiles = (Get-DbaDbFile -SqlInstance $SQLInstance -Database $($EnvILTSILT.Database) | Where-Object {$_.TypeDescription -eq 'LOG'}).LogicalName

    foreach ($Log in $LogFiles) {

        Invoke-DbaQuery -SqlInstance $SQLInstance -Query "DBCC SHRINKFILE (N'$Log', 1)" -Database $($EnvILTSILT.Database)
        Write-DaPSLogEvent  "LogFile [$Log] - cleared" @Logging
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              DROP ALL DATABASES USERS                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Drop Database Users

    $Database, $Env.ILTDatabase, $Env.ILTDatabaseSILT | ForEach-Object {

        Get-DbaDbUser -SqlInstance $SQLInstance -Database $_ -ExcludeSystemUser | Remove-DbaDbUser
        Write-DaPSLogEvent "Dropped all Live user accounts from $_" @Logging

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              ADD USERS AND PERMISSIONS                                                             #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Add Users and Permissions

    $Database, $Env.ILTDatabase, $Env.ILTDatabaseSILT | ForEach-Object {

        $AccountParameters = @{

            SQLInstance                     = $SQLInstance
            Database                        = $_
            UserAccountsDBO                 = $UserAccountsDBO
            UserAccountsReadAccess          = $UserAccountsReadAccess
            UserAccountsWritePermissions    = $UserAccountsWritePermissions
            UserAccountsExecutePermissions  = $UserAccountsExecutePermissions
            UserAccountsBulkAdmin           = $UserAccountsBulkAdmin
            Logfile                         = $Logging

        }
        $AccountsAppliedTraveller = Reset-DaPSAccountPermissions @AccountParameters

        Write-DaPSLogEvent "All $_ User accounts added" @Logging

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                     ENABLE CDC                                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Enable CDC

    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Database -Query 'EXECUTE sys.sp_cdc_enable_db' -Verbose
    Write-DaPSLogEvent "Enabled CDC in the $Database database" @Logging
    
    #endregion
    
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                 ENABLE CDC - TABLES                                                                #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Enable CDC Tables

    foreach ($Table in $($Params.CDCTables.Split(','))) {

        $EnableCDCTable = "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo',  @source_name   = N'$Table', @role_name = NULL"
        Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Database -Query $EnableCDCTable -Verbose
        Write-DaPSLogEvent "CDC Enabled - $Table Table" @Logging

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                             SET TRAVELLER CONFIGURATION                                                            #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Set Traveller Configuration

    Write-DaPSLogEvent "****************Traveller Configuration Started****************" @Logging
    Write-DaPSLogEvent "Traveller Config Script Folder - $($Env.TravellerConfigScripts)" @Logging

    if ($TravellerConfigScripts) {

        Invoke-DbaQuery -SqlInstance $SQLInstance -Query "ALTER DATABASE [$Database] SET CURSOR_DEFAULT  GLOBAL WITH NO_WAIT" -Database $Database -Verbose

        foreach ($Script in $TravellerConfigScripts) {

            Invoke-DbaQuery -SqlInstance $SQLInstance -File $Script.FullName -Database $Database -Verbose -QueryTimeout ([int]::MaxValue)
            Write-DaPSLogEvent "Traveller Config Script Ran - $($Script.Name)" @Logging
        }

    }else {

        Update-DaTravellerApplicationConfiguration -SQLInstance $SQLInstance -TravellerDatabase $Database -logfile $logfile

    }

    Write-DaPSLogEvent "****************Traveller Configuration Completed****************" @Logging

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                     SET SILT DATABASE TO SIMPLE RECOVERY MODEL                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Set SILT Database to Simple Recovery Model

    Set-DbaDbRecoveryModel -SqlInstance $SQLInstance -Database $Env.ILTDatabaseSILT -RecoveryModel Simple
    Write-DaPSLogEvent  "$($Env.ILTDatabaseSILT) set to SIMPLE Recovery" @Logging

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                SHRINK TRAVELLER LOGS                                                               #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    Resize-DaPSTravellerLogs -SqlInstance $SQLInstance -TravellerDatabase $Database -Logging $Logging

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                  REMOVE SILT VIEW                                                                  #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Remove SILT View

    $Env.ILTDatabase, $Env.ILTDatabaseSILT | ForEach-Object {

        $ILTCheck = Remove-DaPSFusionILTView  $SQLInstance  $_  $Params.ILTView
        Write-DaPSLogEvent  $ILTCheck @Logging
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                    TEAMS NOTIFICATION - REPLCATION SETUP STARTED                                                   #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Teams Notification - Replication Setup

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Environment Refresh" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text "Replication Setup Started" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good
            New-AdaptiveFactSet {

                New-AdaptiveFact -Title "Publication" -Value $($Env.PublicationName)
                New-AdaptiveFact -Title "Snapshot Folder" -Value $($Env.SnapShotFolder)

            }  -Separator Medium

        }
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                         CREATE PUBLICATION - TRAVELLER MAIN                                                        #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Create Publication - Traveller Main

    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Database -File $TravellerReplicationILTSDLC -Verbose -QueryTimeout ([int]::MaxValue)
    Write-DaPSLogEvent "[$Database] Publication [$($Env.TravellerPublicationILT)] created" @Logging

    Start-Sleep 10

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                            SET SNAPSHOT FOLDER LOCATION                                                            #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Set Snapshot Folder Location

    $SetSnapshotDefaultFolder = "exec sp_changedistpublisher @publisher = '$($Env.Server)', @property = 'working_directory', @value = '$($Env.SnapShotFolder)'"
    Invoke-DbaQuery -SqlInstance $SQLInstance -Database distribution -Query $SetSnapshotDefaultFolder -Verbose
    Write-DaPSLogEvent "Set Snapshot Folder Location - [$($Env.SnapShotFolder)]" @Logging

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              START SNAPSHOT AGENT JOB                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Start Snapshot Agent Job
    start-sleep 3
    $SnapshotAgentJob = Get-DbaAgentJob -SqlInstance $($Env.SQLInstance) -Category 'REPL-Snapshot' | Where-Object { $_.Name -match $($Env.Database) }  | Sort-Object -Property 'CreateDate' -Descending | 
                        Where-Object { $_.Name -match $($Env.TravellerPublicationILT.Substring(0,20)) } | Select-Object -First 1 | Start-DbaAgentJob -Verbose
    Write-DaPSLogEvent "Snapshot Agent Started - [$($SnapshotAgentJob.Name)]" @Logging
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                     MONITOR REPILCATION INITIALIZATION PROCESS                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Monitor Replication Initialization

    Start-Sleep 60

    Get-DaPSSnapshotStatus  $SQLInstance  $($Env.Server)  $Logging
    Write-DaPSLogEvent "*************Snapshot Created : Syncronisation Started*************" @Logging

    # ------------------------------------------------------ Teams Notification - Snapshot Created ----------------------------------------------------- #
    #region Teams Notification - Snapshot Created

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Environment Refresh" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text "Replication Snapshot Created" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good

        }
    }

    #endregion

    Start-Sleep 30

    Get-DaPSReplicationStatus  $($Env.Server) $SQLInstance $($Env.Database) $($Env.TravellerPublicationILT) $($Env.ILTDatabaseSILT) $Logging
    Write-DaPSLogEvent "*************Replication Setup Completed*************" @Logging

    # ---------------------------------------- Teams Notification - Replication Setup Completed [Traveller Main] --------------------------------------- #
    #region Teams Notification - Replication Setup Completed

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Environment Refresh" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text "Replication Setup Completed" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Good
            New-AdaptiveFactSet {

                New-AdaptiveFact -Title "Publication" -Value $($Env.PublicationName)
                # New-AdaptiveFact -Title "Snapshot Folder" -Value $($Env.SnapShotFolder)

            }  -Separator Medium


        }
    }

    #endregion


    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                               ENABLE SQL/AD ACCOUNTS                                                               #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Enable SQL/AD Accounts

    $AccountsAccess = ($AllUserAccounts | Select-Object -Unique User | Where-Object {$_ -notmatch 'VRUKL'} ).User

    $AccountsAccess | ForEach-Object {

        Set-DbaLogin -SqlInstance $SQLInstance -Login $_ -GrantLogin
        Set-DbaLogin -SqlInstance $SQLInstance -Login $_ -Enable
        Write-DaPSLogEvent  "SQL Account Enabled - [$_]" @Logging
    }
    Write-DaPSLogEvent  "All Accounts Enabled" @Logging

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                        CREATE PUBLICATION - SUPPLIER EXTRAS                                                        #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Create Publication - Supplier Extras

    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Database -File $TravellerReplicationSupplierExtrasSDLC -Verbose -QueryTimeout ([int]::MaxValue)
    Write-DaPSLogEvent "Publication [$($Env.TravellerPublicationILTSupplierExtras)] created" @Logging

    Start-Sleep 15

    $SnapshotAgentJobSupplier = Get-DbaAgentJob -SqlInstance $SQLInstance -Category 'REPL-Snapshot' | Where-Object { $_.Name -match $Database } | Sort-Object -Property 'CreateDate' -Descending | Select-Object -First 1 | Start-DbaAgentJob -Verbose
    Write-DaPSLogEvent "Snapshot Agent Started - [$($SnapshotAgentJobSupplier.Name)]" @Logging

    Start-Sleep 60

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                          CREATE VIEW IN THE SILT DATABASE                                                          #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Create View in the SILT Database

    $VwGetFundingForDiscountAll = Find-DbaView -SqlInstance $SQLInstance -Pattern $Params.ILTView -Database $($EnvILTSILT.Database)
    if (!$VwGetFundingForDiscountAll) {

        Invoke-DbaQuery -SqlInstance $SQLInstance -File $ViewCreateScript -Database $($EnvILTSILT.Database) -Verbose
        Write-DaPSLogEvent "View [$($Params.ILTView)] has been created in the $($EnvILTSILT.Database) database" @Logging

    }
    else {

        Write-DaPSLogEvent "View [$($Params.ILTView) already in the $($EnvILTSILT.Database) database" @Logging

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                    APPLY AND CHECK INDEXES IN THE SILT DATABASE                                                    #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Apply and Check Indexes in the SILT Database

    Invoke-DbaQuery -SqlInstance $SQLInstance -File $IndexCreateScriptSupplierExtra -Database $($EnvILTSILT.Database) -Verbose -QueryTimeout ([int]::MaxValue)
    Write-DaPSLogEvent "[$($EnvILTSILT.Database)] Indexes applied to the database - Supplier Extras" @Logging

    Start-Sleep 2

    Invoke-DbaQuery -SqlInstance $SQLInstance -File $IndexCreateScript -Database $($EnvILTSILT.Database) -Verbose -QueryTimeout ([int]::MaxValue)
    Write-DaPSLogEvent "[$($EnvILTSILT.Database)] Indexes applied to the database" @Logging

    Start-Sleep 2

    $Query = "select Name from sys.indexes As Name order by 1 asc"
    $AllIndexes = (Invoke-DbaQuery -SqlInstance $SQLInstance -Database $($EnvILTSILT.Database) -Query $Query -As PSObject).Name
    $ILT_Indexes = Get-Content -Path $IndexList

    foreach ($Index in $ILT_Indexes) {

        if ($AllIndexes.Contains($Index)) {

            Write-DaPSLogEvent "Index check - Index present [$Index]" @Logging

        }
        else {

            Write-DaPSLogEvent "Error - INDEX MISSING - [$Index]" @Logging

        }

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              CREATE PUBLICATION - SILT                                                             #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Create Publication - SILT

    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Env.ILTDatabaseSILT -File $SILTReplicationILTSDLC -Verbose -QueryTimeout ([int]::MaxValue)
    Write-DaPSLogEvent "Publication [$($Env.PublicationNameSILT)] created" @Logging

    Start-Sleep 15

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                BACKUP SILT DATABASE                                                                #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Backup SILT Database

    # --------------------------------------------------------- Clean up previous backup files --------------------------------------------------------- #
    #region Clean up Backups

    $SILTBackups = "$($Params.'Folder-SILT-Backup')\$($Env.ILTDatabaseSILT)"

    if (!(Test-Path $SILTBackups)) {New-Item -ItemType Directory -Path $SILTBackups -Force}

    $CurrentSILTBackups = Get-ChildItem -Path $SILTBackups -Filter *.bak -File
    $CurrentSILTBackups | ForEach-Object {

        Remove-Item $_.FullName -Force
        Write-DaPSLogEvent "Backup File Removed - $($_.Name)" @Logging

    }

    #endregion

    # ---------------------------------------------------- Teams Notification - SILT Backup Started ---------------------------------------------------- #
    #region Teams Notification - SILT Backup Started

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Database Backup Started" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text $($Env.ILTDatabaseSILT) -Subtle -HorizontalAlignment Center -Color Good -Size Large -Spacing None

        }
    }

    #endregion

    # ----------------------------------------------------------------- Backup Database ---------------------------------------------------------------- #
    #region Backup Database

    Write-DaPSLogEvent "[$($Env.ILTDatabaseSILT)] - Database Backup Started...!!!" @Logging
    $BackupSummary = Backup-DbaDatabase -SqlInstance $($Env.SQLInstance) -Database $($Env.ILTDatabaseSILT) -Path $SILTBackups -CompressBackup -Type Full # -Verbose
    Write-DaPSLogEvent "[$($Env.ILTDatabaseSILT)] - Database Backup Completed. Runtime: $($BackupSummary.Duration)" @Logging

    $Files = @()
    for ($i = 0; $i -lt $BackupSummary.FileList.Count; $i++) {

        $File = [PSCustomObject]@{
            # FileType = $BackupSummary.FileList[$i].FileType
            PhysicalName = $BackupSummary.FileList[$i].PhysicalName
            LogicalName = $BackupSummary.FileList[$i].LogicalName

        }
        $Files += $File
    }

    #endregion

    # --------------------------------------------------- Teams Noptification - SILT Backup Completed -------------------------------------------------- #
    #region Teams Notification - SILT Backup Completed

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Database Backup Completed" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text $($Env.ILTDatabaseSILT) -Subtle -HorizontalAlignment Center -Color Good -Size Large -Spacing None

        }
    }-Action {
        New-AdaptiveAction -Title "Backup Summary" -Body   {
            New-AdaptiveTextBlock -Text "Backup Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {

                New-AdaptiveFact -Title 'Duration' -Value $BackupSummary.Duration
                New-AdaptiveFact -Title 'Database' -Value $BackupSummary.Database
                New-AdaptiveFact -Title 'SqlInstance' -Value $BackupSummary.SqlInstance
                New-AdaptiveFact -Title 'TotalSize' -Value "$($BackupSummary.TotalSize)"
                New-AdaptiveFact -Title 'CompressedBackupSize' -Value "$($BackupSummary.CompressedBackupSize)"
                New-AdaptiveFact -Title 'Path' -Value "$($BackupSummary.Path)"
                New-AdaptiveFact -Title 'FileList' -Value "$($BackupSummary.FileList)"


            } -Separator Medium
        }
        New-AdaptiveAction -Title "File List" -Body   {
            New-AdaptiveTextBlock -Text "File List" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left -Spacing None
            New-AdaptiveTable -DataTable $Files -Size Small -Spacing None

        }
        New-AdaptiveAction -Title "Script" -Body   {
            New-AdaptiveTextBlock -Text "Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveTextBlock  -Text $($BackupSummary.Script) -Wrap

        }

    }

    #endregion

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                          RESTORE SILT DATABASE TO THE ILT                                                          #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Restore SILT Database to the ILT

    # ------------------------------------------------------ Teams Notification - Restore Started ------------------------------------------------------ #
    #region Teams Notification - Restore Started

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Database Restore Started" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text $($Env.ILTDatabase) -Subtle -HorizontalAlignment Center -Color Good -Size Large -Spacing None

        }
    }

    #endregion

    # -------------------------------------------------------------- Restore ILT Database -------------------------------------------------------------- #
    #region Restore ILT Database

    Start-Sleep 2

    $RestoreParameters = @{

        SqlInstance                 = $SQLInstance
        DatabaseName                = $Env.ILTDatabase
        Path                        = $SILTBackups
        DestinationDataDirectory    = $EnvILT.DataDirectory
        DestinationLogDirectory     = $EnvILT.LOGDirectory
        DestinationFileSuffix       = $EnvILT.Suffix
        WithReplace                 = $true
        ErrorAction                 = 'Stop'
        WarningAction               = 'Stop'
        WarningVariable             = 'RefreshWarning'

    }

    Write-DaPSLogEvent "[$($Env.ILTDatabase)] - Database Restore Started...!!!" @Logging
    $RestoreSummary = Restore-DbaDatabase @RestoreParameters
    Write-DaPSLogEvent "[$($Env.ILTDatabaseSILT)] - Database Backup Completed. Runtime: $($RestoreSummary.DatabaseRestoreTime)" @Logging

    #endregion

    # ----------------------------------------------------- Teams Notification - Restore Completed ----------------------------------------------------- #
    #region Teams Notification - Restore Completed

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveTextBlock -Text "Database Restore Completed" -Subtle -HorizontalAlignment Center -Spacing None -Size Large -Color Accent
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
            New-AdaptiveTextBlock -Text $($Env.ILTDatabase) -Subtle -HorizontalAlignment Center -Color Good -Size Large -Spacing None

        }
    }-Action {
        New-AdaptiveAction -Title "Restore Summary" -Body   {
            New-AdaptiveTextBlock -Text "Restore Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {

                New-AdaptiveFact -Title 'Duration' -Value $RestoreSummary.DatabaseRestoreTime
                New-AdaptiveFact -Title 'Database' -Value $RestoreSummary.Database
                New-AdaptiveFact -Title 'SqlInstance' -Value $RestoreSummary.SqlInstance
                New-AdaptiveFact -Title 'BackupSize' -Value "$($RestoreSummary.BackupSize)"
                New-AdaptiveFact -Title 'CompressedBackupSize' -Value "$($RestoreSummary.CompressedBackupSize)"
                New-AdaptiveFact -Title 'RestoreDirectory' -Value "$($RestoreSummary.RestoreDirectory)"
                New-AdaptiveFact -Title 'BackupFile' -Value "$($RestoreSummary.BackupFile)"


            } -Separator Medium
        }
        New-AdaptiveAction -Title "Script" -Body   {
            New-AdaptiveTextBlock -Text "Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveTextBlock  -Text $($RestoreSummary.Script) -Wrap

        }

    }

    #endregion

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                         INITIALIZE SILT DATABASE TO THE ILT                                                        #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Initialize SILT Database to the ILT

    $InitializeFromBackup = @"

        USE $($EnvILTSILT.Database)
        exec sp_addsubscription @publication = N'$($Env.PublicationNameSILT)', @subscriber = N'$($Env.Server)', @destination_db = N'$($Env.ILTDatabase)',
        @subscription_type = N'Push',
        @sync_type = N'initialize with backup', --INITIALIZE WITH BACKUP
        @article = N'all', @update_mode = N'read only', @subscriber_type = 0,
        @backupdevicetype ='disk',  --REQUIRED
        @backupdevicename = N'$($RestoreSummary.BackupFile)'

"@
    Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Env.ILTDatabaseSILT -Query $InitializeFromBackup -ErrorAction Stop -QueryTimeout ([int]::MaxValue)  #-WarningAction Stop -WarningVariable InitializationWarning
    Write-DaPSLogEvent "Initialized SILT Database to the ILT" @Logging

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              CHECK REPLICATION STATUS                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Check Replication Status

    start-sleep 20

    $CurrentReplication = Get-DaPSCurrentReplicationSetup -SQLInstance $Env.Server
    $Publications = $CurrentReplication | Where-Object {$_.publisher_db -eq $Database -or $_.publisher_db -eq  $($Env.ILTDatabaseSILT)}


    $ReplicationStatusScript = @"

        SELECT TOP (1) [agent_id]
        ,[runstatus]
        ,[time]
        ,creation_date
        ,DATEPART(MINUTE, DATEADD(ss, duration, '19000101')) as minutes
        ,[comments]
        ,name
        ,publisher_db
        ,subscriber_db
        ,publication
        ,current_delivery_rate
        ,current_delivery_latency
        ,delivered_commands
        ,delivered_transactions
        --,*
        FROM [distribution].[dbo].[MSdistribution_history] MSDH
        join [distribution].[dbo].[MSdistribution_agents] MSDA on MSDH.agent_id = MSDA.id
        --join [distribution].[dbo].[MSrepl_errors] MSERR on MSDH.error_id = MSERR.id
        where MSDA.publisher_db = '$($Env.ILTDatabaseSILT)'
        order by msdh.time desc

"@

    $repl = Invoke-DbaQuery -SqlInstance $SQLInstance -Database distribution -Query $ReplicationStatusScript -Verbose #-As PSObject

    switch ($repl.runstatus) {
        2 { Write-DaPSLogEvent "[Replication Status]: Succeeded | Comments: $($repl.comments)" @Logging}
        3 { Write-DaPSLogEvent "[Replication Status]: In progress | Comments: $($repl.comments)" @Logging}
        4 { Write-DaPSLogEvent "[Replication Status]: Idle | Comments: $($repl.comments)" @Logging}
        5 { Write-DaPSLogEvent "[Replication Status]: Retrying | Comments: $($repl.comments)" @Logging}
        6 { Write-DaPSLogEvent "[Replication Status]: Failed | Comments: $($repl.comments)" @Logging}

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              ENABLE OWNER PAYMENT JOB                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Enable Owner Payment Job

    if ($env.OwnerPaymentJob) {
        
        Set-DbaAgentJob -SqlInstance $SQLInstance -Job $env.OwnerPaymentJob -Enabled
        Write-DaPSLogEvent "[$($Env.OwnerPaymentJob)] SQL job has been enabled" @Logging

    }
    
    #endregion
    
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                              UPDATE DATABASE SETTINGS                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Update Database Settings

    $Databases = ($Database,$($Env.ILTDatabase))

    foreach ($Database in $Databases) {

        Invoke-DbaQuery -SqlInstance $SQLInstance -Query 'exec sp_changedbowner [sa];' -Database $Database -Verbose -QueryTimeout ([int]::MaxValue)
        Write-DaPSLogEvent "$Database - Change Database Owner to SA" @Logging

        Invoke-DbaQuery -SqlInstance $SQLInstance -Query "ALTER AUTHORIZATION ON DATABASE::[$Database] to sa" -Database $Database -Verbose -QueryTimeout ([int]::MaxValue)
        Write-DaPSLogEvent "$Database - Update Authorization on Database to SA" @Logging

        Invoke-DbaQuery -SqlInstance $SQLInstance -Query "ALTER DATABASE [$Database] SET TRUSTWORTHY ON;" -Database $Database -Verbose -QueryTimeout ([int]::MaxValue)
        Write-DaPSLogEvent "$Database - SET TRUSTWORTHY ON" @Logging

        Invoke-DbaQuery -SqlInstance $SQLInstance -Query 'RECONFIGURE with override' -Database $Database -Verbose -QueryTimeout ([int]::MaxValue)
        Write-DaPSLogEvent "*************$Database Database Configured*************" @Logging
    }


    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                           RESTART DISCOUNTENGINE SERVICE                                                           #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Restart DiscountEngine Service

    Get-Service -Name 'DiscountEngineService' | Restart-Service -Force

    Write-DaPSLogEvent '[DiscountEngineService] Service Restarted' @Logging

    Start-Sleep 60

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                          CHECK DISCOUNT ENGINE HAS LOADED                                                          #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Check Discount Engine has Loaded

    foreach ($Db in $Databases) {
        $DE_Details = Invoke-DbaQuery -SqlInstance $SQLInstance -Database $Db -Query 'exec spde_viewcache' -Verbose
        $DE_Status = ($DE_Details | Where-Object Name -EQ 'IsCacheLoaded').Value

        if ($DE_Status -eq 'True') {

            Write-DaPSLogEvent "$Db - Discount Engine is loaded" @Logging

        }
        else {

            Write-DaPSLogEvent "$Db - Discount Engine is not loaded. Please investigate." @Logging

        }
    }


    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                           CLEAN UP PREVIOUS BACKUP FILES                                                           #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Clean up Backups

    $SILTBackups = "$($Params.'Folder-SILT-Backup')\$($Env.ILTDatabaseSILT)"

    if (!(Test-Path $SILTBackups)) {New-Item -ItemType Directory -Path $SILTBackups -Force}

    $CurrentSILTBackups = Get-ChildItem -Path $SILTBackups -Filter *.bak -File
    $CurrentSILTBackups | ForEach-Object {

        Remove-Item $_.FullName -Force
        Write-DaPSLogEvent "Backup File Removed - $($_.Name)" @Logging

    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                        ENVIRONMENT REFRESH COMPLETION STEPS                                                        #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Environment Refresh Completion Steps

    $DurationEnvironmentRefresh = Get-MyTimer -Name $TimerEnvironmentRefresh
    Write-DaPSLogEvent "[$($Env.Database)] Environment has been refreshed. Runtime: $($DurationEnvironmentRefresh.Duration.ToString("hh\:mm\:ss"))" @Logging
    Remove-MyTimer -Name $TimerEnvironmentRefresh

    $TravellerDatabaseRefresh   = Receive-Job -Name "Refresh#$($Env.Database)" -AutoRemoveJob -Wait
    $SILTDatabaseRefresh        = Receive-Job -Name "Refresh#$($EnvILTSILT.Database)" -AutoRemoveJob -Wait

    if ($($Env.BackupType) -eq 'Live') {

        $FULLBackup = $TravellerDatabaseRefresh | Where-Object {$_.BackupFile -match 'FULL'} | Select-Object -Last 1
        $DIFFBackup = $TravellerDatabaseRefresh | Where-Object {$_.BackupFile -match 'DIFF'} | Select-Object -Last 1
        $TRNBackups = $TravellerDatabaseRefresh | Where-Object {$_.BackupFile -match 'TRN'}
        $LastBackup = $TravellerDatabaseRefresh | Select-Object -Last 1
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                 TEAMS NOTIFICATION - ENVIRONMENT REFRESH COMPLETED                                                 #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region Teams Notification - Refresh Completed

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth {
        New-AdaptiveContainer {

            New-AdaptiveContainer {

                New-AdaptiveTextBlock -Text "Environment Refresh Completed" -Size ExtraLarge -Wrap -HorizontalAlignment Center -Color Accent
                New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[12])" -Subtle -HorizontalAlignment Center -Spacing None
                New-AdaptiveTextBlock -Text "$($Env.Database)" -Subtle -HorizontalAlignment Center -Color Good -Size Large -Spacing None

            }
        }
    }-Action {

        switch ($Env.BackupType) {
            Live {

                New-AdaptiveAction -Title "Refresh Process" -Body   {
                    New-AdaptiveTextBlock -Text "Environment Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title '01.' -Value "Check/Remove Replication"
                        New-AdaptiveFact -Title '02.' -Value "Check/Remove Database Snapshots"
                        New-AdaptiveFact -Title '03.' -Value "Disable Owner Payment job: $($Env.OwnerPaymentJob)"
                        New-AdaptiveFact -Title '04.' -Value "Disable CDC"
                        New-AdaptiveFact -Title '05.' -Value "Refresh $($Env.Database) database. Runtime: $($TravellerDatabaseRefresh.DatabaseRestoreTime | Select-Object -Last 1)"
                        New-AdaptiveFact -Title '05.' -Value "Refresh $($Env.ILTDatabaseSILT) database. Runtime: $($SILTDatabaseRefresh.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title '06.' -Value "Enable Change Tracking"
                        New-AdaptiveFact -Title '07.' -Value "Drop/Add User Accounts"
                        New-AdaptiveFact -Title '08.' -Value "Enable CDC"
                        New-AdaptiveFact -Title '09.' -Value "Update $($Env.Database) Traveller Application Settings"
                        # New-AdaptiveFact -Title '10.' -Value "Refresh $($Env.ILTDatabase) database"
                        New-AdaptiveFact -Title '11.' -Value "Create Traveller Publication: [$($Env.PublicationName)]"
                        New-AdaptiveFact -Title '12.' -Value "Setup Replication"
                        New-AdaptiveFact -Title '11.' -Value "Create Traveller Publication: [$($Env.TravellerPublicationILTSupplierExtras)]"
                        New-AdaptiveFact -Title '11.' -Value "Create SILT Publication: [$($Env.PublicationNameSILT)]"
                        New-AdaptiveFact -Title '13.' -Value "Apply Indexes to the $($Env.ILTDatabaseSILT) database"
                        New-AdaptiveFact -Title '13.' -Value "Backup SILT Database - $($Env.ILTDatabaseSILT). Runtime: $($BackupSummary.Duration)"
                        New-AdaptiveFact -Title '13.' -Value "Restore Database [$($Env.ILTDatabaseSILT)] to [$($Env.ILTDatabase)]. Runtime: $($RestoreSummary.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title '13.' -Value "Initialize Replication"
                        New-AdaptiveFact -Title '14.' -Value "Check Traveller Discount Engine has loaded"
                        New-AdaptiveFact -Title '14.' -Value "Complete Runtime: $($DurationEnvironmentRefresh.Duration.ToString("hh\:mm\:ss"))"

                    } -Separator Medium
                }
                New-AdaptiveAction -Title "Refresh Items" -Body   {

                    New-AdaptiveTextBlock -Text "Databases" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Traveller' -Value "$($Env.Database)"
                        New-AdaptiveFact -Title 'SILT' -Value "$($Env.ILTDatabaseSILT)"
                        New-AdaptiveFact -Title "ILT" -Value "$($Env.ILTDatabase)"

                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Replication" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Traveller Publication 1' -Value $Env.TravellerPublicationILT
                        New-AdaptiveFact -Title 'Traveller Publication 2' -Value $Env.TravellerPublicationILTSupplierExtras
                        New-AdaptiveFact -Title 'SILT Publication' -Value $Env.PublicationNameSILT


                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Folder Locations" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Root Refresh Files' -Value $Params.RefreshFilesFolder
                        New-AdaptiveFact -Title 'Replication Live' -Value $Params.'Folder-ReplicationScriptsLive'
                        New-AdaptiveFact -Title 'Replication SDLC' -Value $FolderReplicationScriptsSDLC
                        New-AdaptiveFact -Title 'Traveller Config' -Value $Env.TravellerConfigScripts
                        New-AdaptiveFact -Title 'Traveller Masked' -Value $Params.'BackUpFolder-Masked-Traveller'
                        New-AdaptiveFact -Title 'AWS LogGroup' -Value $Params.LogGroup
                        New-AdaptiveFact -Title 'AWS Stream' -Value $Logging.LogStreamName


                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Traveller Restore Scripts" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left -Separator Default
                    New-AdaptiveLineBreak
                    New-AdaptiveTextBlock -Text "FULL" -Weight Default -Size Medium -Color Good -HorizontalAlignment Left
                    New-AdaptiveTextBlock  -Text $($TravellerDatabaseRefresh.Script| Select-Object -First 1) -Wrap
                    New-AdaptiveLineBreak
                    New-AdaptiveTextBlock -Text "DIFF" -Weight Default -Size Medium -Color Good -HorizontalAlignment Left
                    New-AdaptiveTextBlock  -Text ($($TravellerDatabaseRefresh | Where-Object {$_.BackupFile -match 'DIFF'})).Script -Wrap

                    New-AdaptiveTextBlock -Text "SILT Restore Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left -Separator Default
                        New-AdaptiveLineBreak
                        New-AdaptiveTextBlock  -Text $($SILTDatabaseRefresh.Script) -Wrap

                }
                New-AdaptiveAction -Title "Traveller Refresh Summary" -Body   {
                    New-AdaptiveTextBlock -Text "Database Restore Summary - $($FULLBackup.Database)" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Duration' -Value "$($FULLBackup.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($FULLBackup.SQLInstance)"
                        New-AdaptiveFact -Title "Server" -Value "$($FULLBackup.ComputerName)"

                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Full Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Full Backup' -Value $(($FULLBackup.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Size' -Value "$($FULLBackup.BackupSize)"
                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($FULLBackup.CompressedBackupSize)"
                        New-AdaptiveFact -Title 'Restore Time' -Value "$($FULLBackup.FileRestoreTime)"


                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "DIFF Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'DIFF Backup' -Value $(($DIFFBackup.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Size' -Value "$($DIFFBackup.BackupSize)"
                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($DIFFBackup.CompressedBackupSize)"
                        New-AdaptiveFact -Title 'Restore Time' -Value "$($DIFFBackup.FileRestoreTime)"


                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Log Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Last Log File' -Value $((($TRNBackups | Select-Object -Last 1).BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Log Files Count' -Value $TRNBackups.Count
                        New-AdaptiveFact -Title 'Logs Restore Time' -Value $([timespan]::fromseconds($(($TRNBackups.FileRestoreTime | Measure-Object -Property TotalSeconds -Sum).Sum)).ToString())

                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Database Files" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Traveller_AddData' -Value $Env.Traveller_AddData
                        New-AdaptiveFact -Title 'Traveller_Data' -Value $Env.Traveller_Data
                        New-AdaptiveFact -Title 'Traveller_Data2' -Value $Env.Traveller_Data2
                        New-AdaptiveFact -Title 'Traveller_Log' -Value $Env.Traveller_Log
                        New-AdaptiveFact -Title 'Traveller_Log2' -Value $Env.Traveller_Log2
                        New-AdaptiveFact -Title 'Traveller_Log3' -Value $Env.Traveller_Log3


                    } -Separator Medium


                }
                New-AdaptiveAction -Title "SILT Refresh Summary" -Body   {
                    New-AdaptiveTextBlock -Text "Database Restore Summary - $($SILTDatabaseRefresh.Database)" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Duration' -Value "$($SILTDatabaseRefresh.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($SILTDatabaseRefresh.SQLInstance)"
                        New-AdaptiveFact -Title "Server" -Value "$($Env.Server)"

                    } -Separator Medium
                    New-AdaptiveTextBlock -Text "Refresh Parameters" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Backup File' -Value $(($SILTDatabaseRefresh.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Backup Size' -Value "$($SILTDatabaseRefresh.BackupSize)"
                        New-AdaptiveFact -Title 'Data Directory' -Value "$($EnvILTSILT.DataDirectory)"
                        New-AdaptiveFact -Title 'Log Directory' -Value "$($EnvILTSILT.LOGDirectory)"
                        New-AdaptiveFact -Title 'FileSuffix' -Value "$($EnvILTSILT.Suffix)"


                    } -Separator Medium
                    New-AdaptiveTextBlock -Text "File List" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left -Spacing None -Separator Default
                        New-AdaptiveTable -DataTable $Files -Size Small -Spacing None -HeaderColor Good


                }
                New-AdaptiveAction -Title "Accounts" -Body {

                    New-AdaptiveTextBlock -Text "Account Summary" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveLineBreak
                    foreach ($Account in $AccountsAppliedTraveller) {

                        New-AdaptiveRichTextBlock -Text "$Account" -Weight Lighter -Spacing None

                    }

                }

             }
            Masked {

                New-AdaptiveAction -Title "Refresh Process" -Body   {
                    New-AdaptiveTextBlock -Text "Environment Refresh Summary" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title '01.' -Value "Check/Remove Replication"
                        New-AdaptiveFact -Title '02.' -Value "Check/Remove Database Snapshots"
                        New-AdaptiveFact -Title '03.' -Value "Disable Owner Payment job: $($Env.OwnerPaymentJob)"
                        New-AdaptiveFact -Title '04.' -Value "Disable CDC"
                        New-AdaptiveFact -Title '05.' -Value "Refresh $($Env.Database) database. Runtime: $($TravellerDatabaseRefresh.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title '05.' -Value "Refresh $($Env.ILTDatabaseSILT) database. Runtime: $($SILTDatabaseRefresh.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title '06.' -Value "Enable Change Tracking"
                        New-AdaptiveFact -Title '07.' -Value "Drop/Add User Accounts"
                        New-AdaptiveFact -Title '08.' -Value "Enable CDC"
                        New-AdaptiveFact -Title '09.' -Value "Update $($Env.Database) Traveller Application Settings"
                        New-AdaptiveFact -Title '10.' -Value "Create Traveller Publication: [$($Env.PublicationName)]"
                        New-AdaptiveFact -Title '11.' -Value "Setup Replication"
                        New-AdaptiveFact -Title '12.' -Value "Create Traveller Publication: [$($Env.TravellerPublicationILTSupplierExtras)]"
                        New-AdaptiveFact -Title '13.' -Value "Create SILT Publication: [$($Env.PublicationNameSILT)]"
                        New-AdaptiveFact -Title '14.' -Value "Apply Indexes to the $($Env.ILTDatabaseSILT) database"
                        New-AdaptiveFact -Title '15.' -Value "Backup SILT Database - $($Env.ILTDatabaseSILT). Runtime: $($BackupSummary.Duration)"
                        New-AdaptiveFact -Title '16.' -Value "Restore Database [$($Env.ILTDatabaseSILT)] to [$($Env.ILTDatabase)]. Runtime: $($RestoreSummary.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title '17.' -Value "Initialize Replication"
                        New-AdaptiveFact -Title '18.' -Value "Check Traveller Discount Engine has loaded"
                        New-AdaptiveFact -Title '19.' -Value "Complete Runtime: $($DurationEnvironmentRefresh.Duration.ToString("hh\:mm\:ss"))"

                    } -Separator Medium
                }
                New-AdaptiveAction -Title "Refresh Items" -Body   {

                    New-AdaptiveTextBlock -Text "Databases" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Traveller' -Value "$($Env.Database)"
                        New-AdaptiveFact -Title 'SILT' -Value "$($Env.ILTDatabaseSILT)"
                        New-AdaptiveFact -Title "ILT" -Value "$($Env.ILTDatabase)"

                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Replication" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Traveller Publication 1' -Value $Env.TravellerPublicationILT
                        New-AdaptiveFact -Title 'Traveller Publication 2' -Value $Env.TravellerPublicationILTSupplierExtras
                        New-AdaptiveFact -Title 'SILT Publication' -Value $Env.PublicationNameSILT


                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Traveller Restore Scripts" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left -Separator Default
                    New-AdaptiveLineBreak
                    New-AdaptiveTextBlock  -Text $($TravellerDatabaseRefresh.Script) -Wrap

                    New-AdaptiveTextBlock -Text "SILT Restore Script" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left -Separator Default
                    New-AdaptiveLineBreak
                    New-AdaptiveTextBlock  -Text $($SILTDatabaseRefresh.Script) -Wrap

                }
                New-AdaptiveAction -Title "Traveller Refresh Summary" -Body   {
                    New-AdaptiveTextBlock -Text "Database Restore Summary - $($TravellerDatabaseRefresh.Database)" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Duration' -Value "$($TravellerDatabaseRefresh.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($TravellerDatabaseRefresh.SQLInstance)"
                        New-AdaptiveFact -Title "Server" -Value "$($TravellerDatabaseRefresh.ComputerName)"

                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Full Backup Details" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Full Backup' -Value $(($TravellerDatabaseRefresh.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Size' -Value "$($TravellerDatabaseRefresh.BackupSize)"
                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($TravellerDatabaseRefresh.CompressedBackupSize)"
                        New-AdaptiveFact -Title 'Restore Time' -Value "$($TravellerDatabaseRefresh.FileRestoreTime)"


                    } -Separator Medium

                    New-AdaptiveTextBlock -Text "Database Files" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Traveller_AddData' -Value $Env.Traveller_AddData
                        New-AdaptiveFact -Title 'Traveller_Data' -Value $Env.Traveller_Data
                        New-AdaptiveFact -Title 'Traveller_Data2' -Value $Env.Traveller_Data2
                        New-AdaptiveFact -Title 'Traveller_Log' -Value $Env.Traveller_Log
                        New-AdaptiveFact -Title 'Traveller_Log2' -Value $Env.Traveller_Log2
                        New-AdaptiveFact -Title 'Traveller_Log3' -Value $Env.Traveller_Log3


                    } -Separator Medium


                }
                New-AdaptiveAction -Title "SILT Refresh Summary" -Body   {
                    New-AdaptiveTextBlock -Text "Database Restore Summary - $($SILTDatabaseRefresh.Database)" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Duration' -Value "$($SILTDatabaseRefresh.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title 'SQL Instance' -Value "$($SILTDatabaseRefresh.SQLInstance)"
                        New-AdaptiveFact -Title "Server" -Value "$($Env.Server)"

                    } -Separator Medium
                    New-AdaptiveTextBlock -Text "Refresh Parameters" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'Backup File' -Value $(($SILTDatabaseRefresh.BackupFile | Select-String -Pattern '[^\\]*\.(bak|trn|diff|DIFF)').Matches.Value)
                        New-AdaptiveFact -Title 'Backup Size' -Value "$($SILTDatabaseRefresh.BackupSize)"
                        New-AdaptiveFact -Title 'Data Directory' -Value "$($EnvILTSILT.DataDirectory)"
                        New-AdaptiveFact -Title 'Log Directory' -Value "$($EnvILTSILT.LOGDirectory)"
                        New-AdaptiveFact -Title 'FileSuffix' -Value "$($EnvILTSILT.Suffix)"


                    } -Separator Medium
                    New-AdaptiveTextBlock -Text "File List" -Weight Default -Size Large -Color Accent -HorizontalAlignment Left -Spacing None -Separator Default
                        New-AdaptiveTable -DataTable $Files -Size Small -Spacing None -HeaderColor Good


                }
                New-AdaptiveAction -Title "Accounts" -Body {

                    New-AdaptiveTextBlock -Text "Account Summary" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveLineBreak
                    foreach ($Account in $AccountsAppliedTraveller) {

                        New-AdaptiveRichTextBlock -Text "$Account" -Weight Lighter -Spacing None

                    }

                }
            }
        }

    }

    #endregion

}