param(
    [Parameter(Mandatory=$True, Position=0, ValueFromPipeline=$false)]
    [System.String]
    $url,

    [Parameter(Mandatory=$False, Position=1, ValueFromPipeline=$false)]
    [System.String]
    $token,

    [Parameter(Mandatory=$False, Position=2, ValueFromPipeline=$false)]
    [System.String]
    $folderPath,

    [Parameter(Mandatory=$False, Position=3, ValueFromPipeline=$false)]
    [System.String]
    $deployFolder
)
################# Test Parameters ######################
# $url = "https://adb-################.##.azuredatabricks.net"
# $token = "dapiabc123def456ghi789jkl012lmn345-1"
# $folderPath = "C:\path\to\your\databricks\notebooks\files\"
# $deployPath = "/workspace/path/"
########################################################
$dbrAPI = $url + "/api/"
$apiHeaders = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "Content-Type: multipart/form-data"
}

if($token -eq "" -or $token -eq $null) {
    Write-Host "Databricks workspace not deployed yet or token not set in variables."
    Write-Host "Please run a deploy, create an Access Token, update the DevOps variable databricks-token with the access token, and rerun the deployment to schedule a job."
}
else {
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

    Write-Host "Getting Files to be Deployed..."


    $files = Get-ChildItem -Recurse -Include *.json_, *.py_, *.config_ -Path $folderPath
    
    $workspaceImportUri = $dbrAPI + "2.0/workspace/import"

    foreach($file in $files) {

        $newPath = $($file.Directory).ToString().Replace("\", "/")
        $newFileName = $($file.Name).Substring(0, ($($file.Name).Length - 1))
        $deployPath = $newPath.Substring($newPath.IndexOf($deployFolder), $newPath.Length - $newPath.IndexOf($deployFolder)) + "/" + $newFileName


        Write-Host "Deploying:$($file.Directory)\$($file.Name) to $deployPath"

        $fileContent = Get-Content "$($file.Directory)\$($file.Name)" -Raw

        $fileContent = $fileContent.Replace("`r`n","`n")
        
        $Bytes = [System.Text.Encoding]::GetEncoding('UTF-8').GetBytes($fileContent)
        $encodedContent = [System.Convert]::ToBase64String($Bytes)        

        $dbrsPutJson = '{ "path": "'+$deployPath+'", "content": "'+$encodedContent+'", "language": "AUTO", "overwrite": true, "format": "AUTO" }'

        Invoke-RestMethod -Uri $workspaceImportUri -Method POST -Headers $apiHeaders -Body $dbrsPutJson -UseBasicParsing
    }
}
