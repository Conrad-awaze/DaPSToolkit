<#
.SYNOPSIS
    Adds account permnissions to a database
.EXAMPLE
    $AccountParameters = @{

        SQLInstance                     = $SQLInstance
        Database                        = $Database
        UserAccountsDBO                 = $UserAccountsDBO
        UserAccountsReadAccess          = $UserAccountsReadAccess
        UserAccountsWriteAccess         = $UserAccountsWriteAccess
        UserAccountsExecutePermissions  = $UserAccountsExecuteAccess
        Logfile                         = $Logfile

    }
    Reset-DaAccountPermissions @AccountParameters
#>
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