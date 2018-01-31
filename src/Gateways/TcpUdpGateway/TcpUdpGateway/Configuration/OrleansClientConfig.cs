﻿using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace TcpUdpGateway.Configuration
{
    public class OrleansClientConfig
    {
        public static bool TryStart(string location, string hostname)
        {
            try
            {
                if (Orleans.GrainClient.IsInitialized)
                    return true;

                //IPAddress ip = GetIP(hostname);
                IPAddress ip = System.Net.Dns.GetHostAddresses(hostname)[0];
                var config = new Orleans.Runtime.Configuration.ClientConfiguration();
                config.Gateways.Add(new IPEndPoint(ip, 30000));
                config.OpenConnectionTimeout = TimeSpan.FromMinutes(4);                
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
                        IPAddress address =  hostInfo.AddressList[index];
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
