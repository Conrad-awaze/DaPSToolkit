function Wait-DaPSAction {
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

        [Parameter()]
        [ValidateNotNullOrEmpty()]
        [int]$RetryInterval
    )
    try {

        # https://mcpmag.com/articles/2018/03/16/wait-action-function-powershell.aspx
        $timer = [Diagnostics.Stopwatch]::StartNew()

        while (($timer.Elapsed.TotalSeconds -lt $Timeout) -and ((& $Condition $ArgumentList) -eq $true)) {

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