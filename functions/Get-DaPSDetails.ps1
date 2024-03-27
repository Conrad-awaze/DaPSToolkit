function Get-DaPSDetails {
    param (

        $Script

    )

    $PSDetails = [PSCustomObject]@{

        PSScriptFileName        = ([regex]::Matches($Script , '([^\\]+$)')).Value 
        PSScriptFileLocation    = ([regex]::Matches($Script , '(^.+[\\])')).Value
        PSScriptRunAs           = "$($env:USERDOMAIN)\$($env:USERNAME)"
        PSVersion               = $PSVersionTable.PSVersion.ToString()
        BuildVersion            = $PSVersionTable.BuildVersion.ToString()
        CLRVersion              = $PSVersionTable.CLRVersion.ToString()
        COMPUTERNAME            = $env:COMPUTERNAME
        OS                      = $env:OS

    }

    $PSDetails
    
    
}