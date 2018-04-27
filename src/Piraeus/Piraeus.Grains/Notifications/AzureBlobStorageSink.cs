using Piraeus.Core.Messaging;
using Piraeus.Core.Metadata;
using SkunkLab.Protocols.Coap;
using SkunkLab.Protocols.Mqtt;
using SkunkLab.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Web;


namespace Piraeus.Grains.Notifications
{
    public class AzureBlobStorageSink : EventSink
    {

        
        private string container;
        private string blobType;
        private string appendFilename;
        private Auditor auditor;
        private Uri uri;
        private string key;
        private Uri sasUri;        
        private BlobStorage[] storageArray;
        private int arrayIndex;
        private ConcurrentQueue<byte[]> queue;
        private int delay;
        private int clientCount;



        public AzureBlobStorageSink(SubscriptionMetadata metadata)
            : base(metadata)
        {

            queue = new ConcurrentQueue<byte[]>();
            auditor = new Auditor();
            key = metadata.SymmetricKey;
            uri = new Uri(metadata.NotifyAddress);
            NameValueCollection nvc = HttpUtility.ParseQueryString(uri.Query);
            container = nvc["container"];
            
            if(!int.TryParse(nvc["clients"], out clientCount))
            {
                clientCount = 1;
            }

            if(!int.TryParse(nvc["delay"], out delay))
            {
                delay = 1000;
            }


            if (String.IsNullOrEmpty(container))
            {
                container = "$Root";
            }

            string btype = nvc["blobtype"];            

            if(String.IsNullOrEmpty(btype))
            {
                blobType = "block";
            }
            else
            {
                blobType = btype.ToLowerInvariant();
            }

            if (blobType != "block" &&
                blobType != "page" &&
                blobType != "append")
            {
                Trace.TraceWarning("Subscription {0} blob storage sink has invalid Blob Type of {1}", metadata.SubscriptionUriString, blobType);
                return;
            }

            sasUri = null;
            Uri.TryCreate(metadata.SymmetricKey, UriKind.Absolute, out sasUri);

            //storageArray = new BlobStorage[rangeMax + 1];
            storageArray = new BlobStorage[clientCount];
            if (sasUri == null)
            {
                string connectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};", uri.Authority.Split(new char[] { '.' })[0], key);
                
                for(int i=0;i<clientCount;i++)
                {
                    storageArray[i] = BlobStorage.New(connectionString, 2048, 102400);
                }
                //storage = BlobStorage.New(connectionString, 2048, 102400);
                //pool = new StoragePool(connectionString, 5, 2048, 102400);

                //storage = BlobStorage.New(String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};", uri.Authority.Split(new char[] { '.' })[0], key), 10240, 1024);
               
                //storageArray[rangeIndex] = BlobStorage.New(String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};", uri.Authority.Split(new char[] { '.' })[0], key), 10240, 1024);
                //for (int i = 0; i < rangeMax; i++)
                //{
                    
                //    //storageArray[rangeIndex] = BlobStorage.New(String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};", uri.Authority.Split(new char[] { '.' })[0], key), 10240, 1024);
                //    //rangeIndex = rangeIndex.RangeIncrement(rangeMin, rangeMax);
                    
                    
                //}
                //storageArray[0] = BlobStorage.New(String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};", uri.Authority.Split(new char[] { '.' })[0], key), 10240, 1024);
                //storageArray[1] = BlobStorage.New(String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};", uri.Authority.Split(new char[] { '.' })[0], key), 10240, 1024);
            }
            else
            {
                string connectionString = String.Format("BlobEndpoint={0};SharedAccessSignature={1}", container != "$Root" ? uri.ToString().Replace(uri.LocalPath, "") : uri.ToString(), key);
                //storage = BlobStorage.New(connectionString, 2048, 102400);
                for (int i = 0; i < clientCount; i++)
                {
                    storageArray[i] = BlobStorage.New(connectionString, 2048, 102400);
                }
                //pool = new StoragePool(connectionString, 5, 2048, 102400);
                //storage = BlobStorage.New(connectionString, 10240, 1024);
                //for (int i = 0; i < rangeMax + 1; i++)
                //{
                //    storageArray[rangeIndex] = BlobStorage.New(connectionString, 10240, 1024);
                //    rangeIndex = rangeIndex.RangeIncrement(rangeMin, rangeMax);
                //}


                //storageArray[0] = BlobStorage.New(connectionString,10240,1024);
                //storageArray[1] = BlobStorage.New(connectionString, 10240, 1024);
            }

            //rangeIndex = 0;
        }

        
        public override async Task SendAsync(EventMessage message)
        {
            
            AuditRecord record = null;
            byte[] payload = null;
            string filename = null;

            byte[] msg = GetPayload(message);

            if (msg != null)
            {
                queue.Enqueue(msg);
            }

            try
            {
                while (!queue.IsEmpty)
                {
                    arrayIndex = arrayIndex.RangeIncrement(0, clientCount - 1);
                    queue.TryDequeue(out payload);
                    
                    //payload = GetPayload(message);
                    if (payload == null)
                    {
                        Trace.TraceWarning("Subscription {0} could not write to blob storage sink because payload was either null or unknown protocol type.");
                        return;
                    }

                    filename = GetBlobName(message.ContentType);

                    if (blobType == "block")
                    {

                        await storageArray[arrayIndex].WriteBlockBlobAsync(container, filename, payload, message.ContentType);
                        //await storage.WriteBlockBlobAsync(container, filename, payload, message.ContentType);

                        //await storage.WriteBlockBlobAsync(container, filename, payload, message.ContentType).ContinueWith(async (antecedent) =>
                        //{
                        //    await storage.WriteBlockBlobAsync(container, filename, payload, message.ContentType);
                        //}, TaskContinuationOptions.OnlyOnFaulted).ContinueWith(async (antecedent) =>
                        //{                        
                        //    await storage.WriteBlockBlobAsync(container, filename, payload, message.ContentType);

                        //}, TaskContinuationOptions.OnlyOnFaulted);
                    }
                    else if (blobType == "page")
                    {
                        int pad = payload.Length % 512 != 0 ? 512 - payload.Length % 512 : 0;
                        byte[] buffer = new byte[payload.Length + pad];
                        Buffer.BlockCopy(payload, 0, buffer, 0, payload.Length);
                        //await storage.WritePageBlobAsync(container, filename, buffer, message.ContentType);
                        await storageArray[arrayIndex].WritePageBlobAsync(container, filename, buffer, message.ContentType);
                    }
                    else
                    {
                        //await storage.WriteAppendBlobAsync(container, filename, payload, message.ContentType);
                        await storageArray[arrayIndex].WriteAppendBlobAsync(container, filename, payload, message.ContentType);
                    }


                    if (auditor.CanAudit && message.Audit)
                    {
                        record = new AuditRecord(message.MessageId, uri.Query.Length > 0 ? uri.ToString().Replace(uri.Query, "") : uri.ToString(), "AzureBlob", "AzureBlob", payload.Length, MessageDirectionType.Out, true, DateTime.UtcNow);
                    }

                    await Task.Delay(delay);
                }
            }
            catch (Exception ex)
            {
                
                if (message != null)
                {
                    Trace.TraceWarning("Failed blob write.");
                    Trace.TraceError(ex.Message);
                    record = new AuditRecord(message.MessageId, uri.Query.Length > 0 ? uri.ToString().Replace(uri.Query, "") : uri.ToString(), "AzureBlob", "AzureBlob", payload.Length, MessageDirectionType.Out, false, DateTime.UtcNow, ex.Message);
                }
            }
            finally
            {
                if (record != null && message.Audit && auditor.CanAudit)
                {
                    await auditor.WriteAuditRecordAsync(record);
                }
            }
        }


       




        private byte[] GetPayload(EventMessage message)
        {
            switch(message.Protocol)
            {
                case ProtocolType.COAP:
                    CoapMessage coap = CoapMessage.DecodeMessage(message.Message);
                    return coap.Payload;
                case ProtocolType.MQTT:
                    MqttMessage mqtt = MqttMessage.DecodeMessage(message.Message);
                    return mqtt.Payload;
                case ProtocolType.REST:
                    return message.Message;
                case ProtocolType.WSN:
                    return message.Message;
                default:
                    return null;
            }
        }

        private string GetAppendFilename(string contentType)
        {
            if (appendFilename == null)
            {
                appendFilename = GetBlobName(contentType);
            }

            return appendFilename;
        }

        private string GetBlobName(string contentType)
        {
            string suffix = null;
            if (contentType.Contains("text"))
            {
                suffix = "txt";
            }
            else if (contentType.Contains("json"))
            {
                suffix = "json";
            }
            else if (contentType.Contains("xml"))
            {
                suffix = "xml";
            }

            string filename = DateTime.UtcNow.ToString("yyyy-MM-ddTHH-MM-ss-fffff");
            return suffix == null ? filename : String.Format("{0}.{1}", filename, suffix);
        }
    }
}
