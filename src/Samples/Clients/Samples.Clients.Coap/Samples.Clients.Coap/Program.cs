using Newtonsoft.Json;
using Piraeus.Clients.Coap;
using SkunkLab.Channels;
using SkunkLab.Channels.WebSocket;
using SkunkLab.Protocols.Coap;
using SkunkLab.Security.Tokens;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Security.Claims;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Samples.Clients.Coap
{
    class Program
    {

        static string jsonPayload;
        static int messageType;
        static int channelNo;
        static int index;
        static IChannel channel;
        static CancellationTokenSource source;
        static string contentType = "text/plain";
        static string publishResource;
        static string observeResource;
        static string role;
        static string clientName;
        static PiraeusCoapClient client;
        static ManualResetEventSlim observeAck;
        static bool canSend;
        static long minTicks;
        static int counter;

        static void Main(string[] args)
        {
            source = new CancellationTokenSource();
            WriteHeader();  //descriptive header
            Console.Write("Select message type [1=Json, Enter=Default] ? ");
            messageType = Console.ReadLine() == "1" ? 1 : 0;
            if(messageType == 1)
            {
                contentType = "application/json";
                jsonPayload = RandomString(990);
            }

            SelectClientRole(); //select a role for the client

            string securityToken = GetSecurityToken();  //get the security token with a unique name
            SetResources(); //setup the resources for pub and observe based on role.

            //Note: Must start the Web gateway and/or TCP/UDP gateway to be able to communicate
            SelectChannel(); //pick a channel for communication 

            channel = GetChannel(channelNo, securityToken);
            channel.OnStateChange += Channel_OnStateChange;
            channel.OnError += Channel_OnError;
            channel.OnClose += Channel_OnClose;

            //configure CoAP for the client
            CoapConfig config = new CoapConfig(null, "www.skunklab.io", CoapConfigOptions.NoResponse | CoapConfigOptions.Observe, false, 180.0, 30.0, 1.5, 2, 1, 4.0, 1.0, 100.0);

         
            if (channelNo == 1) //security token is in Authorize http header for Web socket
            {
                client = new PiraeusCoapClient(config, channel);
            }
            else 
            {
                //TCP and UDP channel; security token is passed in the CoAP URI on the first call
                client = new PiraeusCoapClient(config, channel, SecurityTokenType.JWT, securityToken);
            }

            //observe a resource
            try
            {
                Task task = client.ObserveAsync(observeResource, ObserveResource).ContinueWith(WaitForObserveAck).ContinueWith(SendMessages);
                Task.WaitAll(task);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Observe failed");
                Console.WriteLine(ex.InnerException.Message);
                goto endsample;
            }

            source.Cancel();

            endsample:
            Console.WriteLine("client is closed...");
            Console.ReadKey();
        }

        private static IChannel GetChannel(int num, string securityToken)
        {
            Console.Write("Enter hostname, IP, or Enter for localhost ? ");
            string hostnameOrIP = Console.ReadLine();
            IPAddress address = null;
            bool isIP = IPAddress.TryParse(hostnameOrIP, out address);
            string authority = isIP ? address.ToString() : String.IsNullOrEmpty(hostnameOrIP) ? "localhost" : hostnameOrIP;
            

            if (num == 1)
            {
                int port = Convert.ToInt32(ConfigurationManager.AppSettings["localhostPort"]);
                string uriString = authority.Contains("localhost") ?
                                    String.Format("ws://{0}:{1}/api/connect", authority, port) :
                                    String.Format("ws://{0}/api/connect", authority);

                return ChannelFactory.Create(new Uri(uriString), securityToken, "coapv1", new WebSocketConfig(), source.Token);
              
            }
            else if (num == 2)
            {

                IChannel channel = address == null ? 
                                     ChannelFactory.Create(true, authority, 5684, 1024, 2048, source.Token) :
                                     ChannelFactory.Create(true, address, 5684, 1024, 2048, source.Token);

                return channel;
            }
            else if(num == 3)
            {
                Console.Write("Enter UDP local port for this client ? ");
                int port = Int32.Parse(Console.ReadLine());

                if (address != null)
                {
                    IPEndPoint endpoint = new IPEndPoint(address, 5683);
                    return ChannelFactory.Create(port, endpoint, source.Token);
                }
                else
                {
                    return ChannelFactory.Create(port, hostnameOrIP, 5683, source.Token);
                }
            }

            return null;
        }

        private static void Channel_OnClose(object sender, ChannelCloseEventArgs e)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Channel closed");
            Console.ResetColor();
        }

        static void WaitForObserveAck(Task task)
        {            
            Console.Write("Waiting for Observe ACK... ");
            observeAck = new ManualResetEventSlim();
            observeAck.Wait();
        }

        static void PublishResponse(CodeType code, string contentType, byte[] payload)
        {
            // uncomment if you want to see the return codes from requests

            //Console.ForegroundColor = ConsoleColor.Green;
            //Console.WriteLine("Code {0}", code);
            //Console.ResetColor();
        }
        static void ObserveResource(CodeType code, string contentType, byte[] payload)
        {
            Console.ForegroundColor = ConsoleColor.Magenta;
            if (payload != null)
            {
                if (messageType != 1)
                {
                    Console.WriteLine(Encoding.UTF8.GetString(payload));
                }
                else
                {
                    MyMessage mmsg = JsonConvert.DeserializeObject<MyMessage>(Encoding.UTF8.GetString(payload));
                    long ticks = DateTime.UtcNow.Ticks;
                    minTicks = minTicks == 0 ? mmsg.Ticks : minTicks > mmsg.Ticks ? mmsg.Ticks : minTicks;
                    counter++;
                    Console.WriteLine("{0} - {1} - {2}", counter, TimeSpan.FromTicks(ticks - minTicks).TotalMilliseconds, TimeSpan.FromTicks(ticks - mmsg.Ticks).TotalMilliseconds);
                }
            }
            else
            {
                canSend = code == CodeType.Valid;
                Console.ForegroundColor = ConsoleColor.White;
                Console.Write(code);
                Console.ResetColor();
                Console.WriteLine();
                if (code != CodeType.Valid)
                {
                    Console.WriteLine("Code type '{0}' will not let you continue.", code.ToString().ToUpperInvariant());
                }

                observeAck.Set();
            }
            Console.ResetColor();
        }

        static void SendMessages(Task task)
        {
            Console.WriteLine();
            Console.Write("Send messages (Y/N) ? ");
            bool sending = Console.ReadLine().ToLowerInvariant() == "y";

            if (sending)
            {
                Console.Write("Enter number of messages to send ? ");
                int num = Int32.Parse(Console.ReadLine());

                Console.Write("Enter delay between messages in milliseconds ? ");
                int delay = Int32.Parse(Console.ReadLine());

                for (int i = 0; i < num; i++)
                {
                    index++;
                    //send a message to a resource
                    string payloadString = null;
                    if(messageType == 1)
                    {
                        
                        MyMessage mmsg = new MyMessage() { Ticks = DateTime.UtcNow.Ticks, Payload = jsonPayload };
                        payloadString = JsonConvert.SerializeObject(mmsg);

                    }
                    else
                    {
                        payloadString = String.Format("{0} sent message {1}", clientName, index);
                    }

                    //string message = String.Format("{0} sent message {1}", clientName, index);
                                        
                    byte[] payload = Encoding.UTF8.GetBytes(payloadString);
                    
                    Task pubTask = client.PublishAsync(publishResource, contentType, payload, false, PublishResponse);
                    Task.WhenAll(pubTask);

                    if (delay > 0)
                    {
                        Thread.Sleep(delay);
                        //Task.Delay(delay).Wait();
                    }
                }

                SendMessages(task);
            }
        }


        static void SelectClientRole()
        {
            Console.WriteLine();
            Console.Write("Enter Role for this client (A/B) ? ");
            role = Console.ReadLine().ToUpperInvariant();
            if (role != "A" && role != "B")
                SelectClientRole();
        }

        static string GetSecurityToken()
        {
            string nameClaimType = ConfigurationManager.AppSettings["nameClaimType"];
            string roleClaimType = ConfigurationManager.AppSettings["roleClaimType"];

            Console.Write("Enter unique client name ? ");
            clientName = Console.ReadLine();

            List<Claim> claims = new List<Claim>()
            {
                new Claim(nameClaimType, clientName),
                new Claim(roleClaimType, role)
            };

            string audience = ConfigurationManager.AppSettings["audience"];
            string issuer = ConfigurationManager.AppSettings["issuer"];
            string symmetricKey = ConfigurationManager.AppSettings["symmetricKey"];

            return CreateJwt(audience, issuer, claims, symmetricKey, 60.0);
        }

        static void SetResources()
        {
            string resource1 = ConfigurationManager.AppSettings["resource1"];
            string resource2 = ConfigurationManager.AppSettings["resource2"];
            publishResource = role == "A" ? resource1 : resource2;
            observeResource = role == "A" ? resource2 : resource1;
        }
        

        private static void Channel_OnError(object sender, ChannelErrorEventArgs e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e.Error.Message);
            Console.ResetColor();
        }

        private static void Channel_OnStateChange(object sender, ChannelStateEventArgs e)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Channel State {0}",e.State);
            Console.ResetColor();
        }

        static void SelectChannel()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("--- Select Channel ---");
            Console.WriteLine("(1) Web Socket");
            Console.WriteLine("(2) TCP");
            Console.WriteLine("(3) UDP");
            Console.Write("Enter selection # ? ");

            int num = 0;
            if (Int32.TryParse(Console.ReadLine(), out num) && num > 0 && num < 4)
            {
                channelNo = num;
            }
            else
            {
                Console.WriteLine("Try again...");
                SelectChannel();
            }

        }

        public static string CreateJwt(string audience, string issuer, List<Claim> claims, string symmetricKey, double lifetimeMinutes)
        {
            SkunkLab.Security.Tokens.JsonWebToken jwt = new SkunkLab.Security.Tokens.JsonWebToken(new Uri(audience), symmetricKey, issuer, claims, lifetimeMinutes);
            return jwt.ToString();
        }
        static void WriteHeader()
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("--- CoAP Client ---");
            Console.WriteLine("press any key to continue...");
            Console.WriteLine();
            Console.ResetColor();
            Console.ReadKey();
        }

        private static string RandomString(int size)
        {
            StringBuilder builder = new StringBuilder();
            Random random = new Random();
            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
                builder.Append(ch);
            }

            return builder.ToString();
        }


    }
}
