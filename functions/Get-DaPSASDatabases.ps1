function Get-DaPSASDatabases {
    param (
        [string]$SQLInstance
    )

    # ------------------ Load the AnalysisServices Assembly and create a new object ------------------ #

    [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices") | out-null
    $ASServer = New-Object Microsoft.AnalysisServices.Server

    # ------------ Connect to AnalysisServices Instance, get all databases and disconnect ------------ #

    $ASServer.connect($SQLInstance)
    $Databases = $ASServer.get_Databases()
    $ASServer.disconnect()

    # ----------------------------------- Output all the databases ----------------------------------- #


    $Databases

}