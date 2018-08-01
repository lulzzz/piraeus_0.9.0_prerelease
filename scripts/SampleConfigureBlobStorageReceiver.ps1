
#Configure a Storage Account to Receive messages


#IMPORTANT:  Import the Piraeus Powershell Module
import-module C:\_git\dev\piraeus_0.9.0_prerelease\src\Piraeus\Powershell\Piraeus.Module\Piraeus.Module\bin\Release\Piraeus.Module.dll #FULL PATH to Piraeus.Module.dll  located in src\Piraeus\Powershell\Piraeus.Module\Piraeus.Module\bin\Release\Piraeus.Module.dll

#Login to the Management API

#URL of the Piraeus Web Gateway
#If running in Azure use the hostname or IP address of the virtual machine
#If running locally, type "docker inspect webgateway" to obtain the IP address of the web gateway

$url = "http://piraeus.eastus.cloudapp.azure.com"  #Replace with Host name or IP address of the Piraeus Web Gateway
#$url = "http://localhost:1733"

#get a security token for the management API
$token = Get-PiraeusManagementToken -ServiceUrl $url -Key "12345678"


$resource_A = "http://www.skunklab.io/resource-a"
$containerName="resource-a"

$storageAcct="STORAGE_ACCT_NAME"  #If the blob storage endpint is "https://pirstore.blob.core.windows.net/" use "pirstore" as the hostname
$storageKey="STORAGE_ACCT_KEY"  #Security key to blob storage account

#Remove-PiraeusSubscription -ServiceUrl $url -SecurityToken $token -SubscriptionUriString "http://www.skunklab.io/resource-a/a4ef6ad1-8922-40a6-ba5f-91fbeba3d2df"
Add-PiraeusBlobStorageSubscription -ServiceUrl $url -SecurityToken $token -ResourceUriString $resource_A  -BlobType Block -Host $storageAcct -Container $containerName -Key $storageKey -NumClients 6
