using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Piraeus.Core;
using Piraeus.Core.Messaging;
using Piraeus.Core.Metadata;
using Piraeus.Grains;
using Piraeus.Grains.Notifications;
using SkunkLab.Channels;
using SkunkLab.Diagnostics.Logging;
using SkunkLab.Protocols.Coap;
using SkunkLab.Protocols.Coap.Handlers;

namespace Piraeus.Adapters
{
    public class CoapRequestDispatcher : ICoapRequestDispatch
    {
        public CoapRequestDispatcher(CoapSession session, IChannel channel)
        {
            this.channel = channel;
            this.session = session;
            auditor = new Auditor();
            coapObserved = new Dictionary<string, byte[]>();
            coapUnobserved = new HashSet<string>();
            adapter = new OrleansAdapter(session.Identity, channel.TypeId, "CoAP");
            adapter.OnObserve += Adapter_OnObserve;
            Task task = LoadDurablesAsync();
            Task.WhenAll(task);
        }

        private Auditor auditor;
        private OrleansAdapter adapter;
        private IChannel channel;
        private CoapSession session;
        private HashSet<string> coapUnobserved;
        private Dictionary<string, byte[]> coapObserved;
        private bool disposedValue = false; // To detect redundant calls

        /// <summary>
        /// Unsubscribes an ephemeral subscription from a resource.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<CoapMessage> DeleteAsync(CoapMessage message)
        {
            Exception error = null;

            CoapUri uri = new CoapUri(message.ResourceUri.ToString());
            try
            {
                await adapter.UnsubscribeAsync(uri.Resource);
                coapObserved.Remove(uri.Resource);
            }
            catch (AggregateException ae)
            {
                error = ae.Flatten().InnerException;
            }
            catch (Exception ex)
            {
                error = ex;
            }

            if (error == null)
            {
                ResponseMessageType rmt = message.MessageType == CoapMessageType.Confirmable ? ResponseMessageType.Acknowledgement : ResponseMessageType.NonConfirmable;
                return new CoapResponse(message.MessageId, rmt, ResponseCodeType.Deleted, message.Token);
            }
            else
            {
                return new CoapResponse(message.MessageId, ResponseMessageType.Reset, ResponseCodeType.EmptyMessage);
            }
        }

        //private async Task UnsubscribeAsync(string resourceUriString)
        //{
        //    await adapter.UnsubscribeAsync(resourceUriString);
        //    coapObserved.Remove(resourceUriString);
        //}

        /// <summary>
        /// Not implemented in Piraeus
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <remarks>GET is associated with CoAP observe.  If the client does not support this, then
        /// should use PUT to create a subscription.  Therefore, a GET which is not CoAP observe returns RST.</remarks>
        public Task<CoapMessage> GetAsync(CoapMessage message)
        {
            TaskCompletionSource<CoapMessage> tcs = new TaskCompletionSource<CoapMessage>();
            CoapMessage msg = new CoapResponse(message.MessageId, ResponseMessageType.Reset, ResponseCodeType.EmptyMessage, message.Token);
            tcs.SetResult(msg);
            return tcs.Task;

        }

        /// <summary>
        /// CoAP observe, observes a resource for a subscription.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<CoapMessage> ObserveAsync(CoapMessage message)
        {

            if (!message.Observe.HasValue)
            {
                await Log.LogInfoAsync("Coap observe received without Observe flag set, return RST.");
                //RST because GET needs to be observe/unobserve
                return new CoapResponse(message.MessageId, ResponseMessageType.Reset, ResponseCodeType.EmptyMessage);
            }

            CoapUri uri = new CoapUri(message.ResourceUri.ToString());
            ResponseMessageType rmt = message.MessageType == CoapMessageType.Confirmable ? ResponseMessageType.Acknowledgement : ResponseMessageType.NonConfirmable;

            if (!await adapter.CanSubscribeAsync(uri.Resource, channel.IsEncrypted))
            {
                await Log.LogInfoAsync("Coap observe not authorized.");
                //not authorized
                return new CoapResponse(message.MessageId, rmt, ResponseCodeType.Unauthorized, message.Token);
            }

            if (!message.Observe.Value)
            {
                await Log.LogInfoAsync("Coap observe without value, unsubscribing.");
                //unsubscribe
                await adapter.UnsubscribeAsync(uri.Resource);
                coapObserved.Remove(uri.Resource);
            }
            else
            {
                //subscribe
                SubscriptionMetadata metadata = new SubscriptionMetadata()
                {
                    IsEphemeral = true,
                    Identity = session.Identity,
                    Indexes = session.Indexes
                };

                string subscriptionUriString = await adapter.SubscribeAsync(uri.Resource, metadata);
               

                if (!coapObserved.ContainsKey(uri.Resource)) //add resource to observed list
                {
                    coapObserved.Add(uri.Resource, message.Token);
                }
            }

            return new CoapResponse(message.MessageId, rmt, ResponseCodeType.Valid, message.Token);
        }

        private void Adapter_OnObserve(object sender, ObserveMessageEventArgs e)
        {
            //Task t0 = Log.LogInfoAsync("Coap adapter observed incoming message from Piraeus.");
            //Task.WhenAll(t0);

            byte[] message = null;

            if (coapObserved.ContainsKey(e.Message.ResourceUri))
            {
                message = ProtocolTransition.ConvertToCoap(session, e.Message, coapObserved[e.Message.ResourceUri]);
            }
            else
            {
                message = ProtocolTransition.ConvertToCoap(session, e.Message);
            }


            Task task = Send(message,e);
            Task.WhenAll(task);
        }

        private async Task Send(byte[] message, ObserveMessageEventArgs e)
        {  
            AuditRecord record = null;
            try
            {
                await channel.SendAsync(message);
                record = new AuditRecord(e.Message.MessageId, session.Identity, this.channel.TypeId, e.Message.Protocol.ToString(), e.Message.Message.Length, MessageDirectionType.Out, true, DateTime.UtcNow);
            }
            catch(Exception ex)
            {
                record = new AuditRecord(e.Message.MessageId, session.Identity, this.channel.TypeId, e.Message.Protocol.ToString(), e.Message.Message.Length, MessageDirectionType.Out, false, DateTime.UtcNow, ex.Message);
            }
            finally
            {
                if(e.Message.Audit && auditor.CanAudit)
                {
                    await auditor.WriteAuditRecordAsync(record);
                }
            }

      

        }

        /// <summary>
        /// Publishing a message to a resource.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<CoapMessage> PostAsync(CoapMessage message)
        {
            //await Log.LogInfoAsync("Coap post of message received.");

            try
            {
                CoapUri uri = new CoapUri(message.ResourceUri.ToString());
                ResponseMessageType rmt = message.MessageType == CoapMessageType.Confirmable ? ResponseMessageType.Acknowledgement : ResponseMessageType.NonConfirmable;
                ResourceMetadata metadata = await GraphManager.GetResourceMetadataAsync(uri.Resource);

                if (!await adapter.CanPublishAsync(metadata, channel.IsEncrypted))
                {
                    if(metadata.Audit && auditor.CanAudit)
                    {
                        await auditor.WriteAuditRecordAsync(new AuditRecord("XXXXXXXXXXXX", session.Identity, this.channel.TypeId, "CoAP", message.Payload.Length, MessageDirectionType.In, false, DateTime.UtcNow, "Not authorized, missing resource metadata, or channel encryption requirements"));
                    }

                    return new CoapResponse(message.MessageId, rmt, ResponseCodeType.Unauthorized, message.Token);
                }

                string contentType = message.ContentType.HasValue ? message.ContentType.Value.ConvertToContentType() : "application/octet-stream";

                EventMessage msg = new EventMessage(contentType, uri.Resource, ProtocolType.COAP, message.Encode(), DateTime.UtcNow, metadata.Audit);

                if (uri.Indexes == null)
                {
                    await adapter.PublishAsync(msg);
                }
                else
                {
                    List<KeyValuePair<string, string>> indexes = new List<KeyValuePair<string, string>>(uri.Indexes);

                    Task task = Retry.ExecuteAsync(async () =>
                    {
                        await adapter.PublishAsync(msg, indexes);
                    });

                    await Task.WhenAll(task);
                }


                return new CoapResponse(message.MessageId, rmt, ResponseCodeType.Created, message.Token);
            }
            catch(Exception ex)
            {
                Trace.TraceError("ERROR: CoAP publish erorr {0}", ex.Message);
                Trace.TraceError("ERROR: CoAP publish erorr stack trace {0} ", ex.StackTrace);
                throw ex;
            }
        }
        

        /// <summary>
        /// Subscribe message for ephemeral subscription
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task<CoapMessage> PutAsync(CoapMessage message)
        {
            CoapUri uri = new CoapUri(message.ResourceUri.ToString());
            ResponseMessageType rmt = message.MessageType == CoapMessageType.Confirmable ? ResponseMessageType.Acknowledgement : ResponseMessageType.NonConfirmable;

            if (!await adapter.CanSubscribeAsync(uri.Resource, channel.IsEncrypted))
            {
                return new CoapResponse(message.MessageId, rmt, ResponseCodeType.Unauthorized, message.Token);
            }

            if (coapObserved.ContainsKey(uri.Resource) || coapUnobserved.Contains(uri.Resource))
            {
                //resource previously subscribed 
                return new CoapResponse(message.MessageId, rmt, ResponseCodeType.NotAcceptable, message.Token);
            }

            //this point the resource is not being observed, so we can
            // #1 subscribe to it
            // #2 add to unobserved resources (means not coap observed)

            SubscriptionMetadata metadata = new SubscriptionMetadata()
            {
                IsEphemeral = true,
                Identity = session.Identity,
                Indexes = session.Indexes
            };

            string subscriptionUriString = await adapter.SubscribeAsync(uri.Resource, metadata);

            coapUnobserved.Add(uri.Resource);

            return new CoapResponse(message.MessageId, rmt, ResponseCodeType.Created, message.Token);
        }
        

        private async Task LoadDurablesAsync()
        {
            List<string> list = await adapter.LoadDurableSubscriptionsAsync(session.Identity);

            if (list != null)
            {
                coapUnobserved = new HashSet<string>(list);
            }
        }

        #region IDisposable Support


        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    adapter.OnObserve -= Adapter_OnObserve;
                    adapter.Dispose();
                    coapObserved.Clear();
                    coapUnobserved.Clear();
                    coapObserved = null;
                    coapUnobserved = null;
                }

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~CoapRequestDispatcher() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            GC.SuppressFinalize(this);
        }
        #endregion


        



    }
}
