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
using System.Timers;
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
        private int clientCount;
        private string connectionString;



        public AzureBlobStorageSink(SubscriptionMetadata metadata)
            : base(metadata)
        {          
            auditor = new Auditor();
            key = metadata.SymmetricKey;
            uri = new Uri(metadata.NotifyAddress);
            NameValueCollection nvc = HttpUtility.ParseQueryString(uri.Query);
            container = nvc["container"];
            
            if(!int.TryParse(nvc["clients"], out clientCount))
            {
                clientCount = 1;
            }

            if(!string.IsNullOrEmpty(nvc["file"]))
            {
                appendFilename = nvc["file"];
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

            storageArray = new BlobStorage[clientCount];
            if (sasUri == null)
            {
                connectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};", uri.Authority.Split(new char[] { '.' })[0], key);
                
                for(int i=0;i<clientCount;i++)
                {
                    storageArray[i] = BlobStorage.New(connectionString, 2048, 102400);
                }
            }
            else
            {
<<<<<<< HEAD
                string connectionString = String.Format("BlobEndpoint={0};SharedAccessSignature={1}", container != "$Root" ? uri.ToString().Replace(uri.LocalPath, "") : uri.ToString(), key);

=======
                connectionString = String.Format("BlobEndpoint={0};SharedAccessSignature={1}", container != "$Root" ? uri.ToString().Replace(uri.LocalPath, "") : uri.ToString(), key);
             
>>>>>>> origin/master
                for (int i = 0; i < clientCount; i++)
                {
                    storageArray[i] = BlobStorage.New(connectionString, 2048, 102400);
                }
            }
<<<<<<< HEAD
        }

        
=======
        }        

>>>>>>> origin/master
        public override async Task SendAsync(EventMessage message)
        {
            AuditRecord record = null;
            byte[] payload = null;
            try
            {
                arrayIndex = arrayIndex.RangeIncrement(0, clientCount - 1);

                payload = GetPayload(message);
                if (payload == null)
                {
                    Trace.TraceWarning("Subscription {0} could not write to blob storage sink because payload was either null or unknown protocol type.");
                    return;
                }

                string filename = GetBlobName(message.ContentType);

                if (blobType == "block")
                {
                    //await storageArray[arrayIndex].WriteBlockBlobAsync(container, filename, payload, message.ContentType).ContinueWith(async (a) => { await FaultTask(message.MessageId, container, filename, payload, message.ContentType, auditor.CanAudit && message.Audit); }, TaskContinuationOptions.OnlyOnFaulted);
                    Task task = storageArray[arrayIndex].WriteBlockBlobAsync(container, filename, payload, message.ContentType);
                    Task innerTask = task.ContinueWith(async (a) => { await FaultTask(message.MessageId, container, filename, payload, message.ContentType, auditor.CanAudit && message.Audit); }, TaskContinuationOptions.OnlyOnFaulted);
                    await Task.WhenAll(task);

                }
                else if (blobType == "page")
                {
                    int pad = payload.Length % 512 != 0 ? 512 - payload.Length % 512 : 0;
                    byte[] buffer = new byte[payload.Length + pad];
                    Buffer.BlockCopy(payload, 0, buffer, 0, payload.Length);
                    Task task = storageArray[arrayIndex].WritePageBlobAsync(container, filename, buffer, message.ContentType);
                    Task innerTask = task.ContinueWith(async (a) => { await FaultTask(message.MessageId, container, filename, payload, message.ContentType, auditor.CanAudit && message.Audit); }, TaskContinuationOptions.OnlyOnFaulted);
                    await Task.WhenAll(task);
                    //await storageArray[arrayIndex].WritePageBlobAsync(container, filename, buffer, message.ContentType).ContinueWith(async (a) => { await FaultTask(message.MessageId, container, filename, payload, message.ContentType, auditor.CanAudit && message.Audit); }, TaskContinuationOptions.OnlyOnFaulted);
                }
                else
                {
                    if (appendFilename == null)
                    {
                        appendFilename = GetAppendFilename(message.ContentType);
                    }

                    //await storageArray[arrayIndex].WriteAppendBlobAsync(container, appendFilename, payload, message.ContentType).ContinueWith(async (a) => { await FaultTask(message.MessageId, container, appendFilename, payload, message.ContentType, auditor.CanAudit && message.Audit); }, TaskContinuationOptions.OnlyOnFaulted);
                    Task task = storageArray[arrayIndex].WriteAppendBlobAsync(container, appendFilename, payload, message.ContentType);
                    Task innerTask = task.ContinueWith(async (a) => { await FaultTask(message.MessageId, container, appendFilename, payload, message.ContentType, auditor.CanAudit && message.Audit); }, TaskContinuationOptions.OnlyOnFaulted);
                    await Task.WhenAll(task);

                }

                record = new AuditRecord(message.MessageId, uri.Query.Length > 0 ? uri.ToString().Replace(uri.Query, "") : uri.ToString(), "AzureBlob", "AzureBlob", payload.Length, MessageDirectionType.Out, true, DateTime.UtcNow);
            }
            catch(Exception ex)
            {
                Trace.TraceWarning("Initial blob write error {0}", ex.Message);
                record = new AuditRecord(message.MessageId, uri.Query.Length > 0 ? uri.ToString().Replace(uri.Query, "") : uri.ToString(), "AzureBlob", "AzureBlob", payload.Length, MessageDirectionType.Out, false, DateTime.UtcNow, ex.Message);
            }
            finally
            {
                if (auditor.CanAudit && message.Audit && record != null)
                {
                    await auditor.WriteAuditRecordAsync(record);
                }
            }
        }

        private async Task FaultTask(string id, string container, string filename, byte[] payload, string contentType, bool canAudit)
        {
            AuditRecord record = null;

            try
            {
                BlobStorage storage = BlobStorage.New(connectionString, 2048, 102400);

                if (blobType == "block")
                {
                    await storage.WriteBlockBlobAsync(container, filename, payload, contentType);
                }
                else if (blobType == "page")
                {
                    int pad = payload.Length % 512 != 0 ? 512 - payload.Length % 512 : 0;
                    byte[] buffer = new byte[payload.Length + pad];
                    Buffer.BlockCopy(payload, 0, buffer, 0, payload.Length);
                    await storage.WritePageBlobAsync(container, filename, buffer, contentType);
                }
                else
                {
                    await storage.WriteAppendBlobAsync(container, filename, payload, contentType);
                }

                record = new AuditRecord(id, uri.Query.Length > 0 ? uri.ToString().Replace(uri.Query, "") : uri.ToString(), "AzureBlob", "AzureBlob", payload.Length, MessageDirectionType.Out, true, DateTime.UtcNow);
            }
            catch(Exception ex)
            {
                Trace.TraceWarning("Retry Blob failed.");
                Trace.TraceError(ex.Message);
                record = new AuditRecord(id, uri.Query.Length > 0 ? uri.ToString().Replace(uri.Query, "") : uri.ToString(), "AzureBlob", "AzureBlob", payload.Length, MessageDirectionType.Out, false, DateTime.UtcNow, ex.Message);
            }
            finally
            {
                if(canAudit)
                {
                    await auditor.WriteAuditRecordAsync(record);
                }
            }


        }

        


        //private async Task<bool> RetryAsync(EventMessage message)
        //{
        //    try
        //    {
                
        //        BlobStorage storage = BlobStorage.New(connectionString, 2048, 102400);
        //        string filename = GetBlobName(message.ContentType);
        //        byte[] msg = GetPayload(message);

        //        if (blobType == "block")
        //        {
        //            await storage.WriteBlockBlobAsync(container, filename, msg, message.ContentType);
        //        }
        //        else if (blobType == "page")
        //        {
        //            int pad = msg.Length % 512 != 0 ? 512 - msg.Length % 512 : 0;
        //            byte[] buffer = new byte[msg.Length + pad];
        //            Buffer.BlockCopy(msg, 0, buffer, 0, msg.Length);
        //            await storage.WritePageBlobAsync(container, filename, buffer, message.ContentType);
        //        }
        //        else
        //        {
        //            await storage.WriteAppendBlobAsync(container, filename, msg, message.ContentType);
        //        }

                
        //        Trace.TraceInformation("Blob retry complete.");
        //        return true;
        //    }
        //    catch(Exception ex)
        //    {
        //        Trace.TraceWarning("Blob retry failed.");
        //        Trace.TraceError(ex.Message);
        //        return false;
        //    }

        //}

       





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
