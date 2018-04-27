using Orleans;
using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace WebGateway.Security
{
    public class OrleansClientConfig
    {
        private static IClusterClient client;

        public static bool TryStart(string location, string hostname)
        {
            try
            {
                if (Orleans.GrainClient.IsInitialized)
                    return true;

                var config = new Orleans.Runtime.Configuration.ClientConfiguration();
                config.DeploymentId = "PiraeusDeployment";
                config.PropagateActivityId = true;

                var hostEntry = Dns.GetHostEntry(System.Environment.GetEnvironmentVariable("GATEWAY_ORLEANS_SILO_DNS_HOSTNAME"));
                var ip = hostEntry.AddressList[0];
                Trace.TraceWarning("Host Entry IP Address {0}", ip.ToString());
                config.Gateways.Add(new IPEndPoint(ip, 30000));
                client = new ClientBuilder().UseConfiguration(config).Build();
                Task task = client.Connect();
                Task.WaitAll(task);
                
                Orleans.GrainClient.Initialize(config);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("Failed to intiailize orleans client via hostname");
                Trace.TraceError(ex.Message);
            }

            return Orleans.GrainClient.IsInitialized;
        }

        public static bool TryStart(string location)
        {
            try
            {
                if (Orleans.GrainClient.IsInitialized)
                    return true;
               
                var config = Orleans.Runtime.Configuration.ClientConfiguration.LocalhostSilo();
                
                Orleans.GrainClient.Initialize(config);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("Failed to intiailize orleans client via localhost");
                Trace.TraceError(ex.Message);
            }

            return Orleans.GrainClient.IsInitialized;
        }

        private static IPAddress GetIP(string hostname)
        {
            try
            {
                IPHostEntry hostInfo = Dns.GetHostEntry(hostname);
                for (int index = 0; index < hostInfo.AddressList.Length; index++)
                {
                    if (hostInfo.AddressList[index].AddressFamily == AddressFamily.InterNetwork)
                    {
                        IPAddress address = hostInfo.AddressList[index];
                        Console.WriteLine("Hostname {0} with IP {1}", hostname, address.ToString());
                        return address;
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                Trace.TraceWarning(String.Format("Failed to get IP from hostname"));
                throw ex;
            }
        }
    }
}