function Reset-DaPSSDLCDatabase {
    param (

        [string]$SQLInstance,
        [string]$Database,
        [string]$DatabaseLive,
        [string]$ExcelfileFolder,
        [string]$ddbTableParameters,
        [string]$LogGroupName,
        [string]$ProfileName,
        $PSDetail

    )

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                     PARAMETERS                                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region PARAMETERS

    $Excelfile  = (Get-ChildItem $ExcelfileFolder | Where-Object Extension -EQ '.xlsx'| Sort-Object $_.CreationTime | Select-Object  -First 1).FullName
    $keyTeams   = @{ PK = 'Teams'; SK = 'ChannelGUIDs'} | ConvertTo-DDBItem
    $Teams      = Get-DDBItem -TableName $ddbTableParameters -Key $keyTeams -ProfileName $ProfileName | ConvertFrom-DDBItem
    $URI        = "https://awazecom.webhook.office.com/webhookb2/$($Teams.RefreshNotificationsGUID1)/IncomingWebhook/$($Teams.RefreshNotificationsGUID2)"
    
    $Logging = @{

        LogGroupName    = $LogGroupName
        LogStreamName   = "DatabaseRefresh $Database $(get-date -format "yyyy-MM-dd HH-mm-ss")"
        ProfileName     = $ProfileName
    }

    New-DaPSLogGroupLogStream $Logging.LogGroupName $Logging.LogStreamName $ProfileName

    Write-DaPSLogEvent "[PowerShell Details] [Script Run As]    : $($PSDetail.PSScriptRunAs)" @Logging
    Write-DaPSLogEvent "[PowerShell Details] [Script Name]      : $($PSDetail.PSScriptFileName)" @Logging
    Write-DaPSLogEvent "[PowerShell Details] [Script Location]  : $($PSDetail.PSScriptFileLocation)" @Logging
    Write-DaPSLogEvent "[PowerShell Details] [PS Version]       : $($PSDetail.PSVersion)" @Logging
    Write-DaPSLogEvent "[PowerShell Details] [OS]               : $($PSDetail.OS)" @Logging
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                             GET THE DATABASE PARAMETERS                                                            #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region GET THE DATABASE PARAMATERS

    if ($DatabaseLive -eq 'TR4_Live') {
        
        $ParametersRefresh      = Import-Excel -Path $Excelfile -WorksheetName 'Parameters -Traveller Refresh' | 
                                Where-Object {$_.SQLInstance -eq $SQLInstance -and $_.Database -eq $Database}
    }
    else {

        $ParametersRefresh      = Import-Excel -Path $Excelfile -WorksheetName 'Parameters - Database Refresh' | 
                                Where-Object {$_.SQLInstance -eq $SQLInstance -and $_.Database -eq $Database}
    }
    
    switch ($ParametersRefresh.BackupType) {

        Live { 

            $DatabaseBackupsLive    = Import-Excel -Path $Excelfile -WorksheetName 'Database Backups - LIVE' | 
                                        Where-Object {$_.Database -eq $DatabaseLive}

         }
        Masked {

            $MaskedBackupFolder = $(($ParametersCommon | Where-Object {$_.Parameter -EQ 'BackUpFolder-Masked-Traveller'}).Value)
            $MaskedBackupFile = Get-ChildItem -Path "$MaskedBackupFolder" -Recurse -File

        }
        
    }
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                               SETUP DATABASE ACCOUNTS                                                              #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region SETUP DATABASE ACCOUNTS

    $AllUserAccounts    = Import-Excel -Path $Excelfile -WorksheetName 'Database Access' | 
                            Where-Object {$_.SQL_Instance -eq $SQLInstance -and $_.Database -eq $Database}

    $UserAccountsDBO                = ($AllUserAccounts | Where-Object {$_.Permissions -eq "DBOwner"}).User
    $UserAccountsReadAccess         = ($AllUserAccounts | Where-Object {$_.Permissions -eq "READ"}).User
    $UserAccountsWritePermissions   = ($AllUserAccounts | Where-Object {$_.Permissions -eq "Write"}).User
    $UserAccountsExecutePermissions = ($AllUserAccounts | Where-Object {$_.Permissions -eq "Execute"}).User
    $UserAccountsBulkAdmin          = ($AllUserAccounts | Where-Object {$_.Permissions -eq "bulkadmin"}).User
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                  SETUP DATABASE PARAMETERS AND REVIEW BACKUP FILES                                                 #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region SETUP DATABASE PARAMETERS AND REVIEW BACKUP FILES

    if ($DatabaseLive -eq 'TR4_Live') {

        $FileStructure = @{

            'Tr@veller_Data'    =	$ParametersRefresh.Traveller_Data
            'Tr@veller_Log'     =	$ParametersRefresh.Traveller_Log
            'Traveller_Data2'   =	$ParametersRefresh.Traveller_Data2
            'Traveller_AddData'	=	$ParametersRefresh.Traveller_AddData
            'Tr@veller_Log2'    =	$ParametersRefresh.Traveller_Log2
            'Traveller_Log3'    =	$ParametersRefresh.Traveller_Log3
        }
        
        switch ($ParametersRefresh.BackupType) {

            Live { 

                $RestoreParameters = @{

                    SqlInstance     = $SQLInstance
                    DatabaseName    = $Database
                    FileMapping     = $FileStructure
                    Path            = $DatabaseBackupsLive.location   
                    WithReplace     = $true
                    Verbose         = $true
        
                }

                Write-DaPSLogEvent "[Database Restore] [$Database] Collecting Backup File Details...!!!!" @Logging
        
                $RefreshScript  = Restore-DbaDatabase @RestoreParameters -OutputScriptOnly
            
                $BackupFiles    = ([regex]::Matches("$RefreshScript" , '\w+\.(bak|DIFF|trn)')).Value
                $LatestLog      = $BackupFiles | Where-Object {$_ -match 'trn'} | Sort-Object -Descending | Select-Object -First 1
                
                Write-DaPSLogEvent "[Database Restore] [$Database] FULL Backup: $($BackupFiles | Where-Object {$_ -match 'bak'})" @Logging
                Write-DaPSLogEvent "[Database Restore] [$Database] DIFF Backup: $($BackupFiles | Where-Object {$_ -match 'diff'})" @Logging
                Write-DaPSLogEvent "[Database Restore] [$Database] Latest Log Backup: $LatestLog" @Logging
                Write-DaPSLogEvent "[Database Restore] [$Database] Log Backup Count: $($BackupFiles.Count -2)" @Logging

             }

            Masked {

                $RestoreParameters = @{

                    SqlInstance     = $SQLInstance
                    DatabaseName    = $Database
                    FileMapping     = $FileStructure
                    Path            = $MaskedBackupFile.FullName   
                    WithReplace     = $true
                    Verbose         = $true
        
                }
            }
            
        }
        
    }
    else {

        $RestoreParameters = @{

            SqlInstance                 = $SQLInstance
            DatabaseName                = $Database
            WithReplace                 = $true
            DestinationDataDirectory    = $ParametersRefresh.DataDirectory
            DestinationLogDirectory     = $ParametersRefresh.LOGDirectory
            DestinationFileSuffix       = $ParametersRefresh.Suffix
            Path                        = $DatabaseBackupsLive.location

        }

        Write-DaPSLogEvent  "[Database Restore] [$Database] Collecting Backup File Details...!!!!" @Logging

        try {

            $RefreshScript  = Restore-DbaDatabase @RestoreParameters -OutputScriptOnly -WarningAction Stop -WarningVariable RestoreWarning

        }
        catch {
            
            Write-DaPSLogEvent "**********Database Refresh Error**********"
            Write-DaPSLogEvent "$_"

            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

            New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth  {

                New-AdaptiveContainer {
                    
                    New-AdaptiveTextBlock -Text "Database Refresh Error" -Size ExtraLarge -Wrap -Color Attention -HorizontalAlignment Center
                    New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[13])" -Subtle -Spacing None -Wrap -HorizontalAlignment Center
                    New-AdaptiveTextBlock -Text $Database -Size Medium -Color Good -HorizontalAlignment Center
                    New-AdaptiveFactSet {
                        
                        New-AdaptiveFact -Title 'SQL Instance' -Value $SQLInstance
                        
                    } -Separator Medium -Height Stretch
                      
                }
                
            } -Action {

                New-AdaptiveAction -Title "Error Message" -Body {

                    New-AdaptiveTextBlock -Text "Error Message" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Center
                    New-AdaptiveTextBlock -Text "$RestoreWarning" -Weight Bolder -Size Medium -Color Default -Wrap
   
                } 
                 
            }

            New-AdaptiveCard -Uri $URIAlert -VerticalContentAlignment center -FullWidth  {

                New-AdaptiveContainer {
                    
                    New-AdaptiveTextBlock -Text "Database Refresh Error" -Size ExtraLarge -Wrap -Color Attention -HorizontalAlignment Center
                    New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[13])" -Subtle -Spacing None -Wrap -HorizontalAlignment Center
                    New-AdaptiveTextBlock -Text $Database -Size Medium -Color Good -HorizontalAlignment Center
                    New-AdaptiveFactSet {
                        
                        New-AdaptiveFact -Title 'SQL Instance' -Value $SQLInstance
                        
                    } -Separator Medium -Height Stretch
                    
                }
                
            } -Action {

                New-AdaptiveAction -Title "Error Message" -Body {

                    New-AdaptiveTextBlock -Text "Error Message" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Center
                    New-AdaptiveTextBlock -Text "$RestoreWarning" -Weight Bolder -Size Medium -Color Default -Wrap
   
                } 
                 
            }

            break

        }

        $BackupFiles    = ([regex]::Matches("$RefreshScript" , '\w+\.(bak|DIFF|trn)')).Value
        $LatestLog      = $BackupFiles | Where-Object {$_ -match 'trn'} | Sort-Object -Descending | Select-Object -First 1
        
        Write-DaPSLogEvent "[Database Restore] [$Database] FULL Backup: $($BackupFiles | Where-Object {$_ -match 'bak'})" @Logging

        if (($BackupFiles | Measure-Object).Count -gt 1) {
            
            Write-DaPSLogEvent "[Database Restore] [$Database] DIFF Backup: $($BackupFiles | Where-Object {$_ -match 'diff'})" @Logging
            Write-DaPSLogEvent "[Database Restore] [$Database] Latest Log Backup: $LatestLog" @Logging
            Write-DaPSLogEvent "[Database Restore] [$Database] Log Backup Count: $(($BackupFiles | Measure-Object).Count -2)" @Logging
        }

    }
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                     TEAMS NOTIFICATION - START DATABASE REFRESH                                                    #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region TEAMS NOTIFICATION - START DATABASE REFRESH
    
    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth  {

        New-AdaptiveContainer {
            
            New-AdaptiveTextBlock -Text "Database Refresh Started" -Size ExtraLarge -Wrap -Color Accent -HorizontalAlignment Center
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[13])" -Subtle -Spacing None -Wrap -HorizontalAlignment Center 
            New-AdaptiveTextBlock -Text $Database -Size Large -Color Good -HorizontalAlignment Center -Spacing None
            New-AdaptiveFactSet {
                   
                New-AdaptiveFact -Title 'SQL Instance' -Value $SQLInstance
                
            } -Separator Medium -Height Stretch
            
            
        }
        
    } -Action {

        switch ($ParametersRefresh.BackupType) {
            Live {

                New-AdaptiveAction -Title "Live Backup Files" -Body {
                    New-AdaptiveTextBlock -Text "Live Backup Files" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {
                       
                        New-AdaptiveFact -Title 'Full Backup' -Value "$($BackupFiles | Where-Object {$_ -match 'bak'})"

                        if ($BackupFiles.Count -gt 1) {
                            
                            New-AdaptiveFact -Title 'DIFF Backup' -Value $($BackupFiles | Where-Object {$_ -match 'diff'})
                            New-AdaptiveFact -Title 'Last Log File' -Value $LatestLog
                            if ($($BackupFiles.Count -2) -gt 2) {
                                
                                New-AdaptiveFact -Title 'LogFile Count' -Value $($BackupFiles.Count -2) 
            
                            }
                        }

                    } # -Separator Medium
                        
                }

             }
            Masked {

                New-AdaptiveAction -Title "Masked Backup File" -Body {
                    New-AdaptiveTextBlock -Text "Masked Backup Files" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {
                       
                        New-AdaptiveFact -Title 'Full Backup' -Value $MaskedBackupFile.Name
                        
            
                    } # -Separator Medium
                        
                }

            }
        }
        if ($ParametersRefresh.BackupType -eq 'Live') {
            <# Action to perform if the condition is true #>
        }
        
        New-AdaptiveAction -Title "Refresh Parameters" -Body {
            New-AdaptiveTextBlock -Text "Refresh Parameters" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {
               
                if ($DatabaseLive -ne 'TR4_Live') {

                    New-AdaptiveFact -Title 'DataDirectory' -Value "$($ParametersRefresh.DataDirectory)"
                    New-AdaptiveFact -Title 'LogDirectory' -Value "$($ParametersRefresh.LogDirectory)"
                    New-AdaptiveFact -Title 'File Suffix' -Value $($ParametersRefresh.Suffix)

                }else {

                    New-AdaptiveFact -Title 'Tr@veller_Data' -Value "$($ParametersRefresh.Traveller_Data)"
                    New-AdaptiveFact -Title 'Tr@veller_Log' -Value "$($ParametersRefresh.Traveller_Log)"
                    New-AdaptiveFact -Title 'Traveller_Data2' -Value $($ParametersRefresh.Traveller_Data2)
                    New-AdaptiveFact -Title 'Traveller_AddData' -Value "$($ParametersRefresh.Traveller_AddData)"
                    New-AdaptiveFact -Title 'Tr@veller_Log2' -Value "$($ParametersRefresh.Traveller_Log2)"
                    New-AdaptiveFact -Title 'Traveller_Log3' -Value $($ParametersRefresh.Traveller_Log3)

                }
                                
            } # -Separator Medium
                
        }
        New-AdaptiveAction -Title "PS Details" -Body   {
            New-AdaptiveTextBlock -Text "Details" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {
                
                New-AdaptiveFact -Title 'Script Run As' -Value $PSDetail.PSScriptRunAs
                New-AdaptiveFact -Title 'PowerShell Script' -Value $PSDetail.PSScriptFileName
                New-AdaptiveFact -Title 'Script Location' -Value $PSDetail.PSScriptFileLocation
                New-AdaptiveFact -Title 'PowerShell Version' -Value $PSDetail.PSVersion
                # New-AdaptiveFact -Title 'Build Version' -Value $PSDetail.BuildVersion
                # New-AdaptiveFact -Title 'CLR Version' -Value $PSDetail.CLRVersion
                New-AdaptiveFact -Title 'Computer Name' -Value $PSDetail.COMPUTERNAME
                New-AdaptiveFact -Title 'Operating System' -Value $PSDetail.OS
                
            } -Spacing Small
        }
        
    }

    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                  DATABASE RESTORE                                                                  #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region DATABASE RESTORE

    Write-DaPSLogEvent "[Database Restore] [$Database] Restore Started" @Logging

    $RestoreSummary     = Restore-DbaDatabase @RestoreParameters
    $DurationRestore    = ($RestoreSummary  |  Sort-Object -Property BackupFile -Descending | Select-Object -First 1).DatabaseRestoreTime

    Write-DaPSLogEvent "[Database Restore] [$Database] Restore Completed: $DurationRestore" @Logging
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                            SET RECOVERY MODEL TO SIMPLE                                                            #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region SET RECOVERY MODEL TO SIMPLE
    
    $DBState = Set-DbaDbRecoveryModel -SqlInstance $SQLInstance -Database $Database -RecoveryModel Simple -Confirm:$false

    if ($DBState.RecoveryModel -eq 'Simple') {

        Write-DaPSLogEvent "[$Database] Set to Simple Recovery Mode" @Logging

    }
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                                   DROP ALL USERS                                                                   #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region DROP ALL USERS

    $RemovedAccounts = Get-DbaDbUser -SqlInstance $SQLInstance -Database $Database -ExcludeSystemUser | Remove-DbaDbUser
    Write-DaPSLogEvent "[$Database] [Database Cleanup]: $(($RemovedAccounts | Measure-Object).Count) live accounts removed from the database" @Logging
    
    #endregion
    
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                            ADD ACCOUNTS AND PERMISSIONS                                                            #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region ADD ACCOUNTS AND PERMISSIONS

    $AccountParameters = @{

        SQLInstance                     = $SQLInstance
        Database                        = $Database
        UserAccountsDBO                 = $UserAccountsDBO
        UserAccountsReadAccess          = $UserAccountsReadAccess
        UserAccountsWriteAccess         = $UserAccountsWritePermissions
        UserAccountsExecutePermissions  = $UserAccountsExecutePermissions
        UserAccountsBulkAdmin           = $UserAccountsBulkAdmin
        Logging                         = $Logging

    }

    $AccountSummary = Reset-DaPSAccountPermissions @AccountParameters

    #endregion
    
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                           PREPARE REFRESH SUMMARY DETAILS                                                          #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region PREPARE REFRESH SUMMARY DETAILS

    switch ($ParametersRefresh.BackupType) {
        Live { 

            $SummaryFull        = $RestoreSummary | Where-Object {$_.BackupFile -match 'bak'}
            $SummaryDIFF        = $RestoreSummary | Where-Object {$_.BackupFile -match 'DIFF'}
            $SummaryLastLog     = $RestoreSummary | Where-Object {($_.BackupFile -match 'trn')} | Sort-Object -Property BackupFile -Descending | Select-Object -First 1

            $LastRestoredFile   = $RestoreSummary | Sort-Object -Property BackupEndTime -Descending | Select-Object -First 1

            $LogRestoreSummary  = ($RestoreSummary  | Where-Object {$_.BackupFile -match 'trn'}).FileRestoreTime.Seconds | Measure-Object -Sum
            $LogRestoreDuration = [timespan]::FromSeconds($LogRestoreSummary.Sum).ToString("hh\:mm\:ss") 

         }
        Masked {

            $SummaryFull        = $RestoreSummary | Where-Object {$_.BackupFile -match 'bak'}

        }
    
    }
    
    #endregion

    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #                                                      TEAMS NOTIFICATION - SEND REFRESH SUMMARY                                                     #
    # -------------------------------------------------------------------------------------------------------------------------------------------------- #
    #region TEAMS NOTIFICATION - SEND REFRESH SUMMARY

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls12

    New-AdaptiveCard -Uri $URI -VerticalContentAlignment center -FullWidth  {

        New-AdaptiveContainer {
            
            New-AdaptiveTextBlock -Text "Database Refresh Completed" -Size ExtraLarge -Wrap -Color Accent -HorizontalAlignment Center
            New-AdaptiveTextBlock -Text "$((Get-Date).GetDateTimeFormats()[13])" -Subtle -Spacing None -Wrap -HorizontalAlignment Center
            New-AdaptiveTextBlock -Text $Database -Size Large -Color Good -HorizontalAlignment Center -Spacing None
            
        }
        
    } -Action {

        New-AdaptiveAction -Title "Refresh Summary" -Body {

            New-AdaptiveTextBlock -Text "Refresh Summary" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {
               
                New-AdaptiveFact -Title 'SQL Instance' -Value $SQLInstance
                New-AdaptiveFact -Title 'Database' -Value $Database
                
                switch ($ParametersRefresh.BackupType) {
                    Live { 

                        New-AdaptiveFact -Title 'Duration Complete' -Value "$($LastRestoredFile.DatabaseRestoreTime)"
                        New-AdaptiveFact -Title 'Duration FULL' -Value "$($SummaryFull.FileRestoreTime.ToString("hh\:mm\:ss"))" 

                        if ($BackupFiles.Count -gt 1) {
                            
                            New-AdaptiveFact -Title 'Duration DIFF' -Value "$($SummaryDIFF.FileRestoreTime.ToString("hh\:mm\:ss"))"

                            if ($LastRestoredFile.BackupFile -match '.trn') {

                                New-AdaptiveFact -Title 'Duration Logs' -Value $LogRestoreDuration
                                New-AdaptiveFact -Title 'Logs Restored' -Value $LogRestoreSummary.Count
                                
                            }
                        }

                        New-AdaptiveFact -Title 'DataDirectory' -Value "$($ParametersRefresh.DataDirectory)"
                        New-AdaptiveFact -Title 'LogDirectory' -Value "$($ParametersRefresh.LogDirectory)"

                     }
                    Masked {

                        New-AdaptiveFact -Title 'Duration Masked' -Value "$($SummaryFull.FileRestoreTime.ToString("hh\:mm\:ss"))"

                    }
                    default {

                        New-AdaptiveFact -Title 'Duration Complete' -Value $Duration
                        New-AdaptiveFact -Title 'Duration FULL' -Value "$($SummaryFull.FileRestoreTime.ToString("hh\:mm\:ss"))" 

                        if (condition) {
                            <# Action to perform if the condition is true #>
                        }

                        New-AdaptiveFact -Title 'Duration DIFF' -Value "$($SummaryDIFF.FileRestoreTime.ToString("hh\:mm\:ss"))"

                        if ($LastRestoredFile.BackupFile -match '.trn') {

                            New-AdaptiveFact -Title 'Duration Logs' -Value $LogRestoreDuration
                            New-AdaptiveFact -Title 'Logs Restored' -Value $LogRestoreSummary.Count
                            
                        }

                        New-AdaptiveFact -Title 'DataDirectory' -Value "$($ParametersRefresh.DataDirectory)"
                        New-AdaptiveFact -Title 'LogDirectory' -Value "$($ParametersRefresh.LogDirectory)"
                    }
                }
                                
                
            } -Separator Medium
            if ($DatabaseLive -eq 'TR4_Live') {

                New-AdaptiveTextBlock -Text "Database File Structure" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                New-AdaptiveFactSet {
                
                    New-AdaptiveFact -Title 'Traveller_AddData' -Value "$($ParametersRefresh.Traveller_AddData)"
                    New-AdaptiveFact -Title 'Tr@veller_Data' -Value "$($ParametersRefresh.Traveller_Data)"
                    New-AdaptiveFact -Title 'Traveller_Data2' -Value "$($ParametersRefresh.Traveller_Data2)"
                    New-AdaptiveFact -Title 'Tr@veller_Log' -Value "$($ParametersRefresh.Traveller_Log)"
                    New-AdaptiveFact -Title 'Tr@veller_Log2' -Value "$($ParametersRefresh.Traveller_Log2)"
                    New-AdaptiveFact -Title 'Traveller_Log3' -Value "$($ParametersRefresh.Traveller_Log3)"
                    
                } -Separator Medium

            }
                  
        }
        
        New-AdaptiveAction -Title "Backup Files" -Body {
            
            New-AdaptiveTextBlock -Text "FULL Backup" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {

                switch ($ParametersRefresh.BackupType) {
                    Live { 

                        New-AdaptiveFact -Title 'File' -Value $(([regex]::Matches("$($SummaryFull.BackupFile)" , '\w+\.(bak|DIFF|trn)')).Value)
                        New-AdaptiveFact -Title 'Restore Duration' -Value "$($SummaryFull.FileRestoreTime.ToString("hh\:mm\:ss"))" 
                        New-AdaptiveFact -Title 'Size' -Value "$($SummaryFull.BackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($SummaryFull.CompressedBackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Backup Time' -Value "$($SummaryFull.BackupStartTime)"

                     }
                    Masked {

                        New-AdaptiveFact -Title 'File' -Value $($MaskedBackupFile.Name)
                        New-AdaptiveFact -Title 'Restore Duration' -Value "$($SummaryFull.FileRestoreTime.ToString("hh\:mm\:ss"))" 
                        New-AdaptiveFact -Title 'Size' -Value "$($SummaryFull.BackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($SummaryFull.CompressedBackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Backup Time' -Value "$($SummaryFull.BackupStartTime)"

                    }
                    default {

                        New-AdaptiveFact -Title 'File' -Value $(([regex]::Matches("$($SummaryFull.BackupFile)" , '\w+\.(bak|DIFF|trn)')).Value)
                        New-AdaptiveFact -Title 'Restore Duration' -Value "$($SummaryFull.FileRestoreTime.ToString("hh\:mm\:ss"))" 
                        New-AdaptiveFact -Title 'Size' -Value "$($SummaryFull.BackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($SummaryFull.CompressedBackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Backup Time' -Value "$($SummaryFull.BackupStartTime)"

                    }
                }

                
            } -Separator Medium

            if ($SummaryDIFF) {
                
                New-AdaptiveTextBlock -Text "DIFF Backup" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                New-AdaptiveFactSet {

                    New-AdaptiveFact -Title 'File' -Value $(([regex]::Matches("$($SummaryDIFF.BackupFile)" , '\w+\.(bak|DIFF|trn)')).Value)
                    New-AdaptiveFact -Title 'Restore Duration' -Value "$($SummaryDIFF.FileRestoreTime.ToString("hh\:mm\:ss"))" 
                    New-AdaptiveFact -Title 'Size' -Value "$($SummaryDIFF.BackupSizeMB) MB"
                    New-AdaptiveFact -Title 'Size Compressed' -Value "$($SummaryDIFF.CompressedBackupSizeMB) MB"
                    New-AdaptiveFact -Title 'Backup Time' -Value "$($SummaryDIFF.BackupStartTime)"
                    
                } -Separator Medium

                if ($SummaryLastLog.BackupFile -match '.trn') {
                    
                    New-AdaptiveTextBlock -Text "Latest Log Backup" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                    New-AdaptiveFactSet {

                        New-AdaptiveFact -Title 'File' -Value $(([regex]::Matches("$($SummaryLastLog.BackupFile)" , '\w+\.(bak|DIFF|trn)')).Value)
                        New-AdaptiveFact -Title 'Restore Duration' -Value "$($SummaryLastLog.FileRestoreTime.ToString("hh\:mm\:ss"))" 
                        New-AdaptiveFact -Title 'Size' -Value "$($SummaryLastLog.BackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Size Compressed' -Value "$($SummaryLastLog.CompressedBackupSizeMB) MB"
                        New-AdaptiveFact -Title 'Backup Time' -Value "$($SummaryLastLog.BackupStartTime)"
                        
                    } -Separator Medium
                }
            }
 
        }

        New-AdaptiveAction -Title "Accounts" -Body {
            
            New-AdaptiveTextBlock -Text "Account Summary" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveLineBreak
            foreach ($Account in $AccountSummary) {

                New-AdaptiveRichTextBlock -Text "$Account" -Weight Lighter -Spacing None
                
            } 
  
        }
        
        New-AdaptiveAction -Title "Restore Scripts" -Body {
            
            New-AdaptiveTextBlock -Text "Full Restore" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveRichTextBlock -Text "$($SummaryFull.Script)" -Weight Lighter -Spacing None

            if ($ParametersRefresh.BackupType -eq 'Live'){

                New-AdaptiveTextBlock -Text "DIFF Restore" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
                New-AdaptiveRichTextBlock -Text "$($SummaryDIFF.Script)" -Weight Lighter -Spacing None

            }

        }
        New-AdaptiveAction -Title "PS Details" -Body   {
            New-AdaptiveTextBlock -Text "Details" -Weight Bolder -Size Large -Color Accent -HorizontalAlignment Left
            New-AdaptiveFactSet {
                
                New-AdaptiveFact -Title 'Script Run As' -Value $PSDetail.PSScriptRunAs
                New-AdaptiveFact -Title 'PowerShell Script' -Value $PSDetail.PSScriptFileName
                New-AdaptiveFact -Title 'Script Location' -Value $PSDetail.PSScriptFileLocation
                New-AdaptiveFact -Title 'PowerShell Version' -Value $PSDetail.PSVersion
                # New-AdaptiveFact -Title 'Build Version' -Value $PSDetail.BuildVersion
                # New-AdaptiveFact -Title 'CLR Version' -Value $PSDetail.CLRVersion
                New-AdaptiveFact -Title 'Computer Name' -Value $PSDetail.COMPUTERNAME
                New-AdaptiveFact -Title 'Operating System' -Value $PSDetail.OS
                
            } -Spacing Small
        }
    }
    
    #endregion
    
}