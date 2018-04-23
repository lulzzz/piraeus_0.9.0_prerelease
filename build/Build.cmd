@echo off
set @version=0.9.3
set @prerelease=prerelease

@echo off 
SET name=%@version%-%@prerelease%
echo %name%


@REM NOTE: This script must be run from a Visual Studio command prompt window

@setlocal
@ECHO off



SET CMDHOME=%~dp0.
if "%FrameworkDir%" == "" set FrameworkDir=%WINDIR%\Microsoft.NET\Framework
if "%FrameworkVersion%" == "" set FrameworkVersion=v4.0.30319

SET MSBUILDEXEDIR=%FrameworkDir%\%FrameworkVersion%
SET MSBUILDEXE=%MSBUILDEXEDIR%\MSBuild.exe

cd %CMDHOME%

cd ..\src

cd CAPL\capl
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\CAPL.Lite.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%" 
msbuild Capl.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Capl.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\Capl.log /flp2:logfile=..\..\..\build\BuildOutput\Capl_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\Capl_warnings.log;warningsonly  
nuget pack .\CAPL.Lite.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\Piraeus\Piraeus.Core
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\Piraeus.Core.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\Nuget\packages"
msbuild Piraeus.Core.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.Core.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\Piraeus.Core.log /flp2:logfile=..\..\..\build\BuildOutput\Piraeus.Core_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\Piraeus.Core_warnings.log;warningsonly  
nuget pack .\Piraeus.Core.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\Piraeus\Piraeus.GrainInterfaces
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\Piraeus.GrainInterfaces.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
powershell.exe ..\..\..\build\UpdateDependency.ps1 ".\Piraeus.GrainInterfaces.nuspec" %name% "Piraeus.Core"
nuget restore -PackagesDirectory "..\..\Nuget\packages"
msbuild Piraeus.GrainInterfaces.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.GrainInterfaces.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\Piraeus.GrainInterfaces.log /flp2:logfile=..\..\..\build\BuildOutput\Piraeus.GrainInterfaces_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\Piraeus.GrainInterfaces_warnings.log;warningsonly  
nuget pack Piraeus.GrainInterfaces.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\SkunkLab\SkunkLab.Storage
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\SkunkLab.Storage\SkunkLab.Storage.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\SkunkLab.Storage\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\Nuget\packages"
msbuild SkunkLab.Storage.sln /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild SkunkLab.Storage.sln /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\SkunkLab.Storage.log /flp2:logfile=..\..\..\build\BuildOutput\SkunkLab.Storage_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\SkunkLab.Storage_warnings.log;warningsonly
nuget pack .\SkunkLab.Storage\SkunkLab.Storage.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\SkunkLab\SkunkLab.Security
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\SkunkLab.Security\SkunkLab.Security.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\SkunkLab.Security\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\Nuget\packages"
msbuild .\SkunkLab.Security\SkunkLab.Security.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild .\SkunkLab.Security\SkunkLab.Security.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\SkunkLab.Security.log /flp2:logfile=..\..\..\build\BuildOutput\SkunkLab.Security_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\SkunkLab.Security_warnings.log;warningsonly
nuget pack .\SkunkLab.Security\SkunkLab.Security.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\Piraeus\Piraeus.Grains
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\Piraeus.Grains.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
powershell.exe ..\..\..\build\UpdateDependency.ps1 ".\Piraeus.Grains.nuspec" %name% "Piraeus.GrainInterfaces"
msbuild Piraeus.Grains.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.Grains.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\Piraeus.Grains.log /flp2:logfile=..\..\..\build\BuildOutput\Piraeus.Grains_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\Piraeus.Grains_warnings.log;warningsonly  
nuget pack Piraeus.Grains.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\SkunkLab\SkunkLab.Diagnostics\SkunkLab.Diagnostics
powershell.exe ..\..\..\..\build\UpdateNuspecVersion.ps1" .\SkunkLab.Diagnostics.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
msbuild SkunkLab.Diagnostics.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild SkunkLab.Diagnostics.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Diagnostics.log /flp2:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Diagnostics_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Diagnostics_warnings.log;warningsonly
nuget pack .\SkunkLab.Diagnostics.nuspec -OutputDirectory "..\..\..\Nuget"

cd ..\..\SkunkLab.Channels\SkunkLab.Channels
powershell.exe ..\..\..\..\build\UpdateNuspecVersion.ps1" .\SkunkLab.Channels.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
powershell.exe ..\..\..\..\build\UpdateDependency.ps1 ".\SkunkLab.Channels.nuspec" %name% "SkunkLab.Diagnostics"
nuget restore -PackagesDirectory "..\..\..\Nuget\packages"
msbuild .\SkunkLab.Channels.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild .\SkunkLab.Channels.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Channels.log /flp2:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Channels_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Channels_warnings.log;warningsonly
nuget pack .\SkunkLab.Channels.nuspec -OutputDirectory "..\..\..\Nuget"

cd ..\..\SkunkLab.Protocols\SkunkLab.Protocols
powershell.exe ..\..\..\..\build\UpdateNuspecVersion.ps1" .\SkunkLab.Protocols.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
powershell.exe ..\..\..\..\build\UpdateDependency.ps1 ".\SkunkLab.Protocols.nuspec "%name% "SkunkLab.Security"
nuget restore -PackagesDirectory "..\..\..\Nuget\packages"
msbuild .\SkunkLab.Protocols.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild .\SkunkLab.Protocols.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Protocols.log /flp2:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Protocols_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\SkunkLab.Protocols_warnings.log;warningsonly
nuget pack .\SkunkLab.Protocols.nuspec -OutputDirectory "..\..\..\Nuget"

cd ..\..\..\Piraeus\Piraeus.Configuration
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\Piraeus.Configuration.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
msbuild Piraeus.Configuration.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.Configuration.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\Piraeus.Configuration.log /flp2:logfile=..\..\..\build\BuildOutput\Piraeus.Configuration_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\Piraeus.Configuration_warnings.log;warningsonly
nuget pack .\Piraeus.Configuration.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\Piraeus\Piraeus.Adapters
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\Piraeus.Adapters.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
powershell.exe ..\..\..\build\UpdateDependency.ps1 ".\Piraeus.Adapters.nuspec" %name% "Piraeus.Grains"
powershell.exe ..\..\..\build\UpdateDependency.ps1 ".\Piraeus.Adapters.nuspec" %name% "Piraeus.Configuration"
powershell.exe ..\..\..\build\UpdateDependency.ps1 ".\Piraeus.Adapters.nuspec" %name% "SkunkLab.Channels"
powershell.exe ..\..\..\build\UpdateDependency.ps1 ".\Piraeus.Adapters.nuspec" %name% "SkunkLab.Protocols"
nuget restore -PackagesDirectory "..\..\Nuget\packages"
msbuild Piraeus.Adapters.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.Adapters.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\Piraeus.Adapters.log /flp2:logfile=..\..\..\build\BuildOutput\Piraeus.Adapters_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\Piraeus.Adapters_warnings.log;warningsonly
nuget pack .\Piraeus.Adapters.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\Piraeus\Piraeus.SiloHost
powershell.exe ..\..\..\build\UpdateNuspecVersion.ps1" .\Piraeus.SiloHost.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
powershell.exe ..\..\..\build\UpdateDependency.ps1 ".\Piraeus.SiloHost.nuspec" %name% "Piraeus.Grains"
nuget restore -PackagesDirectory "..\..\Nuget\packages"
msbuild Piraeus.SiloHost.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.SiloHost.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\Piraeus.SiloHost.log /flp2:logfile=..\..\..\..\build\BuildOutput\Piraeus.SiloHost_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\Piraeus.SiloHost_warnings.log;warningsonly
nuget pack .\Piraeus.SiloHost.nuspec -OutputDirectory "..\..\Nuget"

cd ..\..\Piraeus\Powershell\Piraeus.Module\Piraeus.Module
powershell.exe ..\..\..\..\..\build\UpdateNuspecVersion.ps1" .\Piraeus.Module.nuspec" %@version% %@prerelease%
powershell.exe ..\..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
powershell.exe ..\..\..\..\..\build\UpdateDependency.ps1 ".\Piraeus.Module.nuspec" %name% "Piraeus.Core"
nuget restore -PackagesDirectory "..\..\..\..\Nuget\packages"
msbuild Piraeus.Module.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.Module.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\..\build\BuildOutput\Piraeus.Module.log /flp2:logfile=..\..\..\..\..\build\BuildOutput\Piraeus.Module_errors.log;errorsonly /flp3:logfile=..\..\..\..\..\build\BuildOutput\Piraeus.Module_warnings.log;warningsonly
nuget pack .\Piraeus.Module.nuspec -OutputDirectory "..\..\..\..\Nuget"

cd ..\..\..\..\SkunkLab\SkunkLab.Servers\SkunkLab.Listeners\SkunkLab.Listteners
powershell.exe ..\..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\..\..\Nuget\packages"
msbuild SkunkLab.Listeners.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild SkunkLab.Listeners.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\..\build\BuildOutput\SkunkLab.Listeners.log /flp2:logfile=..\..\..\..\..\build\BuildOutput\Piraeus.Module_errors.log;errorsonly /flp3:logfile=..\..\..\..\..\build\BuildOutput\SkunkLab.Listeners_warnings.log;warningsonly

cd ..\..\..\..\Gateways\WebGateway\WebGateway
powershell.exe ..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\..\Nuget\packages"
msbuild WebGateway.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild WebGateway.csproj /t:restore,Clean,Rebuild  /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\WebGateway.log /flp2:logfile=..\..\..\..\build\BuildOutput\WebGateway_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\WebGateway_warnings.log;warningsonly /p:DeployOnBuild=true /p:PublishProfile=FolderProfile

cd ..\..\TcpUdpGateway\TcpUdpGateway
powershell.exe ..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\..\Nuget\packages"
msbuild TcpUdpGateway.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild TcpUdpGateway.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\TcpUdpGateway.log /flp2:logfile=..\..\..\..\build\BuildOutput\TcpUdpGateway_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\TcpUdpGateway_warnings.log;warningsonly

cd ..\..\..\SiloHost\Piraeus.Silo\Piraeus.Silo
powershell.exe ..\..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\..\Nuget\packages"
msbuild ..\Piraeus.Silo.sln /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild ..\Piraeus.Silo.sln /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\Piraeus.Silo.log /flp2:logfile=..\..\..\..\build\BuildOutput\Piraeus.Silo_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\Piraeus.Silo_warnings.log;warningsonly

cd ..\..\..\Piraeus\Piraeus.Clients
powershell ..\..\..\build\VersionUpdate.ps1 ".\Properties\AssemblyInfo.cs" "%@version%"
nuget restore -PackagesDirectory "..\..\Nuget\packages"
msbuild Piraeus.Clients.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Piraeus.Clients.csproj /t:Clean,Rebuild,restore /p:Configuration=Debug /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\build\BuildOutput\Piraeus.Clients.log /flp2:logfile=..\..\..\build\BuildOutput\Piraeus.Clients_errors.log;errorsonly /flp3:logfile=..\..\..\build\BuildOutput\Piraeus.Clients_warnings.log;warningsonly

cd ..\..\Samples\Clients\Samples.Clients.Coap\Samples.Clients.Coap
nuget restore -PackagesDirectory "..\..\..\..\Nuget\packages"
msbuild Samples.Clients.Coap.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Samples.Clients.Coap.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\..\build\BuildOutput\Samples.Clients.Coap.log /flp2:logfile=..\..\..\..\..\build\BuildOutput\Samples.Clients.Coap_errors.log;errorsonly /flp3:logfile=..\..\..\..\..\build\BuildOutput\Samples.Clients.Coap_warnings.log;warningsonly

cd ..\..\Samples.Clients.Mqtt
nuget restore -PackagesDirectory "..\..\..\Nuget\packages"
msbuild Samples.Clients.Mqtt.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Samples.Clients.Mqtt.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\Samples.Clients.Mqtt.log /flp2:logfile=..\..\..\..\build\BuildOutput\Sample.Clients.Mqtt_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\Samples.Clients.Mqtt_warnings.log;warningsonly

cd ..\Samples.Clients.Rest
nuget restore -PackagesDirectory "..\..\..\Nuget\packages"
msbuild Samples.Clients.Rest.csproj /t:Clean,Rebuild,restore /p:configuration=Debug
msbuild Samples.Clients.Rest.csproj /t:Clean,Rebuild,restore /p:Configuration=Release /fl1 /fl2 /fl3 /flp1:logfile=..\..\..\..\build\BuildOutput\Samples.Clients.Rest.log /flp2:logfile=..\..\..\..\build\BuildOutput\Samples.Clients.Rest_errors.log;errorsonly /flp3:logfile=..\..\..\..\build\BuildOutput\Samples.Clients.Rest_warnings.log;warningsonly

cd ..\..\..\..\build\

powershell -command .\builddocker.ps1

:ENDLINE