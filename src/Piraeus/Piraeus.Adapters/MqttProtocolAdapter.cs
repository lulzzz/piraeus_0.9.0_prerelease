using Piraeus.Configuration.Settings;
using Piraeus.Core;
using Piraeus.Core.Messaging;
using Piraeus.Core.Metadata;
using Piraeus.Grains;
using Piraeus.Grains.Notifications;
using SkunkLab.Channels;
using SkunkLab.Diagnostics.Logging;
using SkunkLab.Protocols.Mqtt;
using SkunkLab.Protocols.Mqtt.Handlers;
using SkunkLab.Security.Authentication;
using SkunkLab.Security.Identity;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security;
using System.Threading;
using System.Threading.Tasks;


namespace Piraeus.Adapters
{
    public class MqttProtocolAdapter : ProtocolAdapter
    {
        public MqttProtocolAdapter(PiraeusConfig config, IAuthenticator authenticator, IChannel channel)
        {
            this.config = config;
            MqttConfig mqttConfig = new MqttConfig(authenticator, config.Protocols.Mqtt.KeepAliveSeconds,
                   config.Protocols.Mqtt.AckTimeoutSeconds, config.Protocols.Mqtt.AckRandomFactor, config.Protocols.Mqtt.MaxRetransmit, config.Protocols.Mqtt.MaxLatencySeconds);
            mqttConfig.IdentityClaimType = config.Identity.Client.IdentityClaimType;
            mqttConfig.Indexes = config.Identity.Client.Indexes;

            session = new MqttSession(mqttConfig);           

            Channel = channel;
            Channel.OnClose += Channel_OnClose;
            Channel.OnError += Channel_OnError;
            Channel.OnStateChange += Channel_OnStateChange;
            Channel.OnReceive += Channel_OnReceive;
            Channel.OnOpen += Channel_OnOpen;
        }

        public override event System.EventHandler<ProtocolAdapterErrorEventArgs> OnError;
        public override event System.EventHandler<ProtocolAdapterCloseEventArgs> OnClose;
        public override event System.EventHandler<ChannelObserverEventArgs> OnObserve;

        private Auditor auditor;
        private MqttSession session;
        private bool disposed;
        private OrleansAdapter adapter;
        private PiraeusConfig config;
        private bool forcePerReceiveAuthn;



        public override IChannel Channel { get; set; }


        public override void Init()
        {
            auditor = new Auditor();

            forcePerReceiveAuthn = Channel as UdpChannel != null;
            session.OnPublish += Session_OnPublish;
            session.OnSubscribe += Session_OnSubscribe;
            session.OnUnsubscribe += Session_OnUnsubscribe;
            session.OnDisconnect += Session_OnDisconnect; ;
            session.OnConnect += Session_OnConnect;            
        }


        #region Dispose 
        protected void Disposing(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    adapter.Dispose();
                    session.Dispose();
                }

                disposed = true;
            }
        }

        public override void Dispose()
        {
            Disposing(true);
            GC.SuppressFinalize(this);
        }
        
        #endregion
        

        #region Orleans Adapter Events
        private void Adapter_OnObserve(object sender, ObserveMessageEventArgs e)
        {  
            AuditRecord record = null;
            int length = 0;
            DateTime sendTime = DateTime.UtcNow;
            try
            {
                byte[] message = ProtocolTransition.ConvertToMqtt(session, e.Message);
                Task task = Send(message);
                Task.WhenAll(task);

                MqttMessage mm = MqttMessage.DecodeMessage(message);

                length = mm.Payload.Length;
                record = new AuditRecord(e.Message.MessageId, session.Identity, this.Channel.TypeId, e.Message.Protocol.ToString(), length, MessageDirectionType.Out, true, sendTime);
            }
            catch(Exception ex)
            {
                Trace.TraceWarning("MQTT Adapter observe fault.");
                Trace.TraceError("MQTT Adapter observe erorr {0}", ex.Message);
                Trace.TraceError("MQTT Adapter observer stack trace {0}", ex.StackTrace);
                record = new AuditRecord(e.Message.MessageId, session.Identity, this.Channel.TypeId, e.Message.Protocol.ToString(), length, MessageDirectionType.Out, true, sendTime, ex.Message);
            }
            finally
            {
               if(auditor.CanAudit && e.Message.Audit)
                {
                    Task task = auditor.WriteAuditRecordAsync(record);
                    Task.WhenAll(task);
                }
            }
        }

        private async Task Send(byte[] message)
        {
            try
            {
                await Channel.SendAsync(message);
            }
            catch(Exception ex)
            {
                Trace.TraceWarning("MQTT Adapter send fault.");
                Trace.TraceError("MQTT Adpater send error {0}", ex.Message);
                Trace.TraceError("MQTT Adapter send stack trace {0}", ex.StackTrace);
            }
        }

        #endregion

        #region MQTT Session Events
        private void Session_OnConnect(object sender, MqttConnectionArgs args)
        {
            try
            {
                if (args.Code == ConnectAckCode.ConnectionAccepted)
                {
                    Task t = adapter.LoadDurableSubscriptionsAsync(session.Identity);
                    Task.WhenAll(t);
                }

                Task task = Log.LogInfoAsync("Mqtt session is connected for channel {0}", Channel.Id);
                Task.WhenAll(task);
            }
            catch(Exception ex)
            {
                Task logTask = Log.LogErrorAsync("Mqtt OnConnect error {0}.", ex.Message);
                Task.WhenAll(logTask);
                OnError?.Invoke(this, new ProtocolAdapterErrorEventArgs(Channel.Id, ex));
                Task task = Channel.CloseAsync();
                Task.WhenAll(task);
            }
        }

        private void Session_OnDisconnect(object sender, MqttMessageEventArgs args)
        {
            Task task = Log.LogErrorAsync("Mqtt Channel {0} OnDisconnect.", Channel.Id);
            Task.WhenAll(task);
            Task closeTask = Channel.CloseAsync();
            Task.WhenAll(closeTask);
        }

        private void Session_OnUnsubscribe(object sender, MqttMessageEventArgs args)
        {
            try
            {
                UnsubscribeMessage msg = (UnsubscribeMessage)args.Message;
                foreach (var item in msg.Topics)
                {
                    Task task = Task.Factory.StartNew(async () =>
                    {
                        MqttUri uri = new MqttUri(item.ToLowerInvariant());
                        if (await adapter.CanSubscribeAsync(uri.Resource, Channel.IsEncrypted))
                        {
                            await adapter.UnsubscribeAsync(uri.Resource);
                        }
                    });

                    Task.WhenAll(task);
                }
            }
            catch(Exception ex)
            {
                Task task = Log.LogErrorAsync("Mqtt On_Unsubscribe error {0}", ex.Message);
                Task.WhenAll(task);
                OnError?.Invoke(this, new ProtocolAdapterErrorEventArgs(Channel.Id, ex));
            }
        }

        private List<string> Session_OnSubscribe(object sender, MqttMessageEventArgs args)
        {
            List<string> list = new List<string>();

            try
            {
                
                SubscribeMessage message = args.Message as SubscribeMessage;

                SubscriptionMetadata metadata = new SubscriptionMetadata()
                {
                    Identity = session.Identity,
                    Indexes = session.Indexes,
                    IsEphemeral = true
                };

                foreach (var item in message.Topics)
                {
                    MqttUri uri = new MqttUri(item.Key);
                    string resourceUriString = uri.Resource;

                    Task<bool> t = CanSubscribe(resourceUriString);
                    bool subscribe = t.Result;

                    if (subscribe)
                    {
                        Task<string> subTask = Subscribe(resourceUriString, metadata);
                        string subscriptionUriString = subTask.Result;
                        list.Add(resourceUriString);
                    }
                }
            }
            catch(Exception ex)
            {
                Task task = Log.LogErrorAsync("Mqtt On_Subscribe error {0}", ex.Message);
                Task.WhenAll(task);
                OnError?.Invoke(this, new ProtocolAdapterErrorEventArgs(Channel.Id, ex));
                Task closeTask = Channel.CloseAsync();
                Task.WhenAll(closeTask);
            }

            return list;
        }

        private Task<string> Subscribe(string resourceUriString, SubscriptionMetadata metadata)
        {
            TaskCompletionSource<string> tcs = new TaskCompletionSource<string>();
            Task t = Task.Factory.StartNew(async () =>
            {
                string id = await adapter.SubscribeAsync(resourceUriString, metadata);
                tcs.SetResult(id);
            });

            return tcs.Task;           
        }

        private Task<bool> CanSubscribe(string resourceUriString)
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();

            Task t = Task.Factory.StartNew(async () =>
            {
                bool r = await adapter.CanSubscribeAsync(resourceUriString, Channel.IsEncrypted);
                tcs.SetResult(r);
            });

            return tcs.Task;
        }

        private void Session_OnPublish(object sender, MqttMessageEventArgs args)
        {
            try
            {
                PublishMessage message = args.Message as PublishMessage;

                MqttUri mqttUri = new MqttUri(message.Topic);

                Task task = Retry.ExecuteAsync(async () =>
                {
                    ResourceMetadata metadata = await GraphManager.GetResourceMetadataAsync(mqttUri.Resource);

                    if (await adapter.CanPublishAsync(metadata, Channel.IsEncrypted))
                    {
                        EventMessage msg = new EventMessage(mqttUri.ContentType, mqttUri.Resource, ProtocolType.MQTT, message.Encode(), DateTime.UtcNow, metadata.Audit);
                        await adapter.PublishAsync(msg, null);
                    }
                    else
                    {
                        if (metadata.Audit && auditor.CanAudit)
                        {
                            await auditor.WriteAuditRecordAsync(new AuditRecord("XXXXXXXXXXXX", session.Identity, this.Channel.TypeId, "MQTT", args.Message.Payload.Length, MessageDirectionType.In, false, DateTime.UtcNow, "Not authorized, missing resource metadata, or channel encryption requirements"));
                        }

                        await Log.LogWarningAsync("Mqtt message cannot be published because not authorized.");
                    }
                });

                Task.WhenAll(task);

                //Task task = Task.Factory.StartNew(async () =>
                //{
                //    ResourceMetadata metadata = await GraphManager.GetResourceMetadataAsync(mqttUri.Resource);

                //    if (await adapter.CanPublishAsync(metadata, Channel.IsEncrypted))
                //    {
                //        EventMessage msg = new EventMessage(mqttUri.ContentType, mqttUri.Resource, ProtocolType.MQTT, message.Encode(), DateTime.UtcNow, metadata.Audit);
                //        await adapter.PublishAsync(msg, null);
                //    }
                //    else
                //    {
                //        await Log.LogWarningAsync("Mqtt message cannot be published because not authorized.");
                //    }

                //});

                //Task.WhenAll(task);
            }
            catch(Exception ex)
            {
                Trace.TraceError("ERROR: MQTT Publish error {0}", ex.Message);
                Trace.TraceError("ERORR: MQTT Publish error stack trace {0} ", ex.StackTrace);
                OnError?.Invoke(this, new ProtocolAdapterErrorEventArgs(Channel.Id, ex));
                //Task logTask = Log.LogErrorAsync("Mqtt On_Publish error {0}", ex.Message);
                //Task.WhenAll(logTask);
                Task task = Channel.CloseAsync();
                Task.WhenAll(task);
            }
        }

        #endregion        

        #region Channel Events
        private void Channel_OnOpen(object sender, ChannelOpenEventArgs e)
        {
            try
            {
                session.IsAuthenticated = Channel.IsAuthenticated;

                if (session.IsAuthenticated)
                {
                    IdentityDecoder decoder = new IdentityDecoder(session.Config.IdentityClaimType, session.Config.Indexes);
                    session.Identity = decoder.Id;
                    session.Indexes = decoder.Indexes;
                }

                adapter = new OrleansAdapter(session.Identity, Channel.TypeId, "MQTT");
                adapter.OnObserve += Adapter_OnObserve;
            }
            catch(Exception ex)
            {
                OnError?.Invoke(this, new ProtocolAdapterErrorEventArgs(Channel.Id, ex));
                Task logTask = Log.LogErrorAsync("Mqtt Channel OnOpen error {0}", ex.Message);
                Task.WhenAll(logTask);

                Task task = Channel.CloseAsync();
                Task.WhenAll(task);
            }
        }

        private void Channel_OnReceive(object sender, ChannelReceivedEventArgs e)
        {
            LimitedConcurrencyLevelTaskScheduler lcts = new LimitedConcurrencyLevelTaskScheduler(5);
            CancellationTokenSource tokenSource = new CancellationTokenSource();

            try
            {

                MqttMessage msg = MqttMessage.DecodeMessage(e.Message);
                OnObserve?.Invoke(this, new ChannelObserverEventArgs(null, null, e.Message));

                if(!session.IsAuthenticated)
                {
                    ConnectMessage message = msg as ConnectMessage;
                    if(message == null)
                    {
                        throw new SecurityException("Connect message not first message");
                    }

                    if(session.Authenticate(message.Username, message.Password))
                    {
                        IdentityDecoder decoder = new IdentityDecoder(session.Config.IdentityClaimType, session.Config.Indexes);
                        session.Identity = decoder.Id;
                        session.Indexes = decoder.Indexes;
                    }
                    else
                    {
                        throw new SecurityException("Session could not be authenticated.");
                    }
                }
                else if(forcePerReceiveAuthn)
                {
                    if(!session.Authenticate())
                    {
                        throw new SecurityException("Per receive authentication failed.");
                    }
                }
                

                Task task = Task.Factory.StartNew(async () =>
                {
                    MqttMessageHandler handler = MqttMessageHandler.Create(session, msg);
                    MqttMessage message = await handler.ProcessAsync();

                    if (message != null)
                    {
                        await Channel.SendAsync(message.Encode());
                    }

                },tokenSource.Token, TaskCreationOptions.AttachedToParent, lcts).ContinueWith(ExceptionAction, TaskContinuationOptions.OnlyOnFaulted);



                Task.WhenAll(task);

                //Task task = Receive(msg);
                
                
            }            
            catch (Exception ex)
            {
                Task task = Log.LogErrorAsync("Mqtt Channel OnReceive error {0}", ex.Message);
                Task.WhenAll(task);
                Task closeTask = Channel.CloseAsync();
                Task.WhenAll(closeTask);
            }
        }

        private void ExceptionAction(Task task)
        {
            System.Diagnostics.Trace.WriteLine(task.Exception.Message);
            System.Diagnostics.Trace.WriteLine(task.Exception.StackTrace);
        }

        

        //private async Task Receive(MqttMessage msg)
        //{
        //    var throttler = new System.Threading.SemaphoreSlim(1);
        //    await throttler.WaitAsync();


        //    TaskCompletionSource<Task> tcs = new TaskCompletionSource<Task>();            

        //    try
        //    {
        //        MqttMessageHandler handler = MqttMessageHandler.Create(session, msg);
        //        MqttMessage message = await handler.ProcessAsync();

        //        if (message != null)
        //        {
        //            await Channel.SendAsync(message.Encode());                   
        //        }
        //    }
        //    catch(Exception ex)
        //    {
        //        await Log.LogWarningAsync("Message received not processed.");
        //        await Log.LogErrorAsync(ex.Message);
        //        throw ex;
        //    }

        //    tcs.SetResult(null);

        //    throttler.Release();

        //    await Task.FromResult<Task>(tcs.Task);
        //}

        private void Channel_OnStateChange(object sender, ChannelStateEventArgs e)
        {
            Task task = Log.LogInfoAsync("Mqtt Channel {0} state changed to {1}", Channel.Id, e.State);
            Task.WhenAll(task);
        }

       

        private void Channel_OnError(object sender, ChannelErrorEventArgs e)
        {           
            OnError(this, new ProtocolAdapterErrorEventArgs(Channel.Id, e.Error));
            Task task = Log.LogErrorAsync("Mqtt Channel {0} as error {1}", Channel.Id, e.Error.Message);
            Task.WhenAll(task);

            Task closeTask = Channel.CloseAsync();
            Task.WhenAll(closeTask);
        }

        private void Channel_OnClose(object sender, ChannelCloseEventArgs e)
        {
            Task task = Log.LogAsync("Channel {0} is closed.", e.ChannelId);
            Task.WhenAll(task);

            OnClose?.Invoke(this, new ProtocolAdapterCloseEventArgs(e.ChannelId));
        }

        #endregion

        


    }
}
