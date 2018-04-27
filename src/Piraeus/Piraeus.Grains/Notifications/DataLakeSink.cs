//using Microsoft.Rest.Azure.Authentication;
//using Piraeus.Core.Messaging;
//using Piraeus.Core.Metadata;
//using SkunkLab.Protocols.Coap;
//using SkunkLab.Protocols.Mqtt;
//using System;
//using System.Collections.Specialized;
//using System.Diagnostics;
//using System.IO;
//using System.Threading;
//using System.Threading.Tasks;
//using System.Web;

//namespace Piraeus.Grains.Notifications
//{
//    public class DataLakeSink : EventSink
//    {

//        private string appId;
//        private string secret;
//        private string domain;
//        private string account;
//        private string folder;
//        private string filename;
//        private string subscriptionUriString;
//        private Uri ADL_TOKEN_AUDIENCE = new System.Uri(@"https://datalake.azure.net/");
//        private DataLakeStoreFileSystemManagementClient client;

//        /// <summary>
//        /// Creates Azure Data Lake notification 
//        /// </summary>
//        /// <param name="contentType"></param>
//        /// <param name="messageId"></param>
//        /// <param name="metadata"></param>
//        /// <remarks>adl://host.azuredatalakestore.net?appid=id&tenantid=id&secret=token&folder=name</remarks>
//        public DataLakeSink(SubscriptionMetadata metadata)
//            : base(metadata)
//        {
//            try
//            {
//                subscriptionUriString = metadata.SubscriptionUriString;

//                Uri uri = new Uri(metadata.NotifyAddress);
//                account = uri.Authority.Replace(".azuredatalakestore.net", "");
//                NameValueCollection nvc = HttpUtility.ParseQueryString(uri.Query);
//                appId = nvc["appid"];
//                domain = nvc["domain"];
//                folder = nvc["folder"];
//                filename = nvc["file"];

//                secret = metadata.SymmetricKey;

//                var creds = GetCreds_SPI_SecretKey(domain, ADL_TOKEN_AUDIENCE, appId, secret);
//                client = new DataLakeStoreFileSystemManagementClient(creds);


//            }
//            catch (Exception ex)
//            {
//                Trace.TraceWarning("Azure data lake subscription client not created for {0}", subscriptionUriString);
//                Trace.TraceError("Azure data lake subscription {0} ctor error {1}", subscriptionUriString, ex.Message);
//            }
//        }



//        public override async Task SendAsync(EventMessage message)
//        {
//            try
//            {
//                byte[] payload = GetPayload(message);
//                string path = GetPath(message.ContentType);
//                if (filename != null)
//                {
//                    using (MemoryStream stream = new MemoryStream(payload))
//                    {
//                        await client.FileSystem.AppendAsync(account, path, stream);
//                    }
//                }
//            }

//            catch (Exception ex)
//            {
//                Trace.TraceWarning("Data Lake sink write error.");
//                Trace.TraceError("Azure data lake subscription {0} writing error {1}", subscriptionUriString, ex.Message);
//            }
//        }

//        private string GetFilename(string contentType)
//        {
//            string suffix = null;
//            if (contentType.Contains("text"))
//            {
//                suffix = "txt";
//            }
//            else if (contentType.Contains("json"))
//            {
//                suffix = "json";
//            }
//            else if (contentType.Contains("xml"))
//            {
//                suffix = "xml";
//            }

//            string filename = DateTime.UtcNow.ToString("yyyy-MM-ddTHH-MM-ss-fffff");
//            return suffix == null ? filename : String.Format("{0}.{1}", filename, suffix);
//        }


//        private byte[] GetPayload(EventMessage message)
//        {
//            switch (message.Protocol)
//            {
//                case ProtocolType.COAP:
//                    CoapMessage coap = CoapMessage.DecodeMessage(message.Message);
//                    return coap.Payload;
//                case ProtocolType.MQTT:
//                    MqttMessage mqtt = MqttMessage.DecodeMessage(message.Message);
//                    return mqtt.Payload;
//                case ProtocolType.REST:
//                    return message.Message;
//                case ProtocolType.WSN:
//                    return message.Message;
//                default:
//                    return null;
//            }
//        }



//        private string GetPath(string contentType)
//        {
//            if (filename != null)
//            {
//                return String.Format("/{0}/{1}", folder, filename);
//            }
//            else
//            {
//                return GetFilename(contentType);
//            }
//        }


//        private ServiceClientCredentials GetCreds_SPI_SecretKey(string tenant, Uri tokenAudience, string clientId, string secretKey)
//        {
//            ServiceClientCredentials scc;

//            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

//            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
//            serviceSettings.TokenAudience = tokenAudience;

//            var creds = ApplicationTokenProvider.LoginSilentAsync(
//             tenant,
//             clientId,
//             secretKey,
//             serviceSettings).GetAwaiter().GetResult();
//            return creds;
//        }




//    }
//}
