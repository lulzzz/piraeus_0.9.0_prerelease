using Capl.Authorization;
using Piraeus.Core.Messaging;
using Piraeus.Core.Metadata;
using Piraeus.GrainInterfaces;
using Piraeus.Grains;
using SkunkLab.Diagnostics.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Piraeus.Grains.Notifications;

namespace Piraeus.Adapters
{
    public class OrleansAdapter : OrleansAdapterBase
    {
        public OrleansAdapter(string identity, string channelType, string protocolType)
        {
            this.identity = identity;
            this.channelType = channelType;
            this.protocolType = protocolType;
            Task task = SetAuditorAsync();
            Task.WhenAll(task);

            container = new Dictionary<string, Tuple<string, string>>();
            ephemeralObservers = new Dictionary<string, IMessageObserver>();
            durableObservers = new Dictionary<string, IMessageObserver>();
        }

        public event EventHandler<ObserveMessageEventArgs> OnObserve;   //signal protocol adapter

        private Auditor auditor;
        private string identity;
        private string channelType;
        private string protocolType;
        private Dictionary<string, Tuple<string, string>> container;  //resource, subscription + leaseKey
        private Dictionary<string, IMessageObserver> ephemeralObservers; //subscription, observer
        private Dictionary<string, IMessageObserver> durableObservers;   //subscription, observer
        private System.Timers.Timer leaseTimer; //timer for leases
        private bool disposedValue = false; // To detect redundant calls

        public override async Task<List<string>> LoadDurableSubscriptionsAsync(string identity)
        {
            List<string> list = new List<string>();

            IEnumerable<string> subscriptionUriStrings = await GraphManager.GetSubscriberSubscriptionsListAsync(identity);

            if (subscriptionUriStrings == null || subscriptionUriStrings.Count() == 0)
            {
                return null;
            }

            foreach (var item in subscriptionUriStrings)
            {
                if (!durableObservers.ContainsKey(item))
                {
                    MessageObserver observer = new MessageObserver();
                    observer.OnNotify += Observer_OnNotify;

                    //set the observer in the subscription with the lease lifetime
                    TimeSpan leaseTime = TimeSpan.FromSeconds(20.0);

                    string leaseKey = await GraphManager.AddSubscriptionObserverAsync(item, leaseTime, observer);

                    //add the lease key to the list of ephemeral observers
                    durableObservers.Add(item, observer);


                    //get the resource from the subscription
                    Uri uri = new Uri(item);
                    string resourceUriString = item.Replace(uri.Segments[uri.Segments.Length - 1], "");

                    list.Add(resourceUriString); //add to list to return

                    //add the resource, subscription, and lease key the container

                    if (!container.ContainsKey(resourceUriString))
                    {
                        container.Add(resourceUriString, new Tuple<string, string>(item, leaseKey));
                    }
                }
            }

            if (subscriptionUriStrings.Count() > 0)
            {
                EnsureLeaseTimer();
            }

            return list.Count == 0 ? null : list;
        }

        public override async Task<bool> CanPublishAsync(ResourceMetadata metadata, bool channelEncrypted)
        {
            //ResourceMetadata metadata = await GraphManager.GetResourceMetadataAsync(resourceUriString);

            if (metadata == null)
            {
                await Log.LogWarningAsync("Publish resource metadata is null.");
                return false;
            }

            if (!metadata.Enabled)
            {
                await Log.LogWarningAsync("Publish resource {0} is disabled.", metadata.ResourceUriString);
                return false;
            }

            if (metadata.Expires.HasValue && metadata.Expires.Value < DateTime.UtcNow)
            {
                await Log.LogWarningAsync("Publish resource {0} has expired.", metadata.ResourceUriString);
                return false;
            }

            if (metadata.RequireEncryptedChannel && !channelEncrypted)
            {
                await Log.LogWarningAsync("Publish resource {0} requires an encrypted channel.", metadata.ResourceUriString);
                return false;
            }

            AuthorizationPolicy policy = await GraphManager.GetAccessControlPolicyAsync(metadata.PublishPolicyUriString);

            if (policy == null)
            {
                await Log.LogWarningAsync("Publish policy URI {0} did not return an authorization policy.", metadata.PublishPolicyUriString);
                return false;
            }

            ClaimsIdentity identity = Thread.CurrentPrincipal.Identity as ClaimsIdentity;
            bool authz = policy.Evaluate(identity);

            if (!authz)
            {                
                await Log.LogWarningAsync("Identity is not authorized to publish to resource {0}", metadata.ResourceUriString);
                foreach (Claim claim in identity.Claims)
                {
                    await Log.LogInfoAsync("Identity claim {0} : {1}", claim.Type, claim.Value);
                }
            }

            return authz;
        }

        public override async Task<bool> CanSubscribeAsync(string resourceUriString, bool channelEncrypted)
        {
            ResourceMetadata metadata = await GraphManager.GetResourceMetadataAsync(resourceUriString);

            if (metadata == null)
            {
                await Log.LogWarningAsync("Cannot subscribe. Resource metadata is null.");
                return false;
            }

            if (!metadata.Enabled)
            {
                await Log.LogWarningAsync("Cannot subscribe. Resource {0} is disabled.", metadata.ResourceUriString);
                return false;
            }

            if (metadata.Expires.HasValue && metadata.Expires.Value < DateTime.UtcNow)
            {
                await Log.LogWarningAsync("Cannot subscribe. Resource {0} has expired.", metadata.ResourceUriString);
                return false;
            }

            if (metadata.RequireEncryptedChannel && !channelEncrypted)
            {
                await Log.LogWarningAsync("Cannot subscribe. Resource {0} requires an encrypted channel.", metadata.ResourceUriString);
                return false;
            }

            AuthorizationPolicy policy = await GraphManager.GetAccessControlPolicyAsync(metadata.SubscribePolicyUriString);
            
            if(policy == null)
            {
                await Log.LogWarningAsync("Subscription policy URI {0} did not return an authorization policy.", metadata.SubscribePolicyUriString);
                return false;
            }

            ClaimsIdentity identity = Thread.CurrentPrincipal.Identity as ClaimsIdentity;
            
            bool authz = policy.Evaluate(identity);

            if (!authz)
            {
                foreach(var claim in identity.Claims)
                {
                    await Log.LogWarningAsync("{0} : {1}", claim.Type, claim.Value);
                }
                await Log.LogWarningAsync("Identity is not authorized to subscribe/unsubscribe for resource {0}", metadata.ResourceUriString);
            }

            return authz;
        }
        
        public override async Task PublishAsync(EventMessage message, List<KeyValuePair<string, string>> indexes = null)
        {
            AuditRecord record = null;

            try
            {
                if (indexes == null || indexes.Count == 0)
                {
                    await GraphManager.PublishAsync(message.ResourceUri, message);                   
                }
                else
                {
                    await GraphManager.PublishAsync(message.ResourceUri, message, indexes);                    
                }

                if(message.Audit)
                {

                    record = new AuditRecord(message.MessageId, identity, channelType, protocolType, message.Message.Length, DateTime.UtcNow);
                }
            }
            catch(Exception ex)
            {
                record = new AuditRecord(message.MessageId, identity, channelType, protocolType, message.Message.Length, DateTime.UtcNow, ex.Message);
            }
            finally
            {
                await AuditAsync(record);
            }
        }

        public override async Task<string> SubscribeAsync(string resourceUriString, SubscriptionMetadata metadata)
        {
            metadata.IsEphemeral = true;
            string subscriptionUriString = await GraphManager.SubscribeAsync(resourceUriString, metadata);

            //create and observer and wire up event to receive notifications
            MessageObserver observer = new MessageObserver();
            observer.OnNotify += Observer_OnNotify;

            //set the observer in the subscription with the lease lifetime
            TimeSpan leaseTime = TimeSpan.FromSeconds(20.0);

            string leaseKey = await GraphManager.AddSubscriptionObserverAsync(subscriptionUriString, leaseTime, observer);

            //add the lease key to the list of ephemeral observers
            ephemeralObservers.Add(subscriptionUriString, observer);

            //add the resource, subscription, and lease key the container
            if (!container.ContainsKey(resourceUriString))
            {
                container.Add(resourceUriString, new Tuple<string, string>(subscriptionUriString, leaseKey));
            }

            //ensure the lease timer is running
            EnsureLeaseTimer();

            return subscriptionUriString;
        }

        public override async Task UnsubscribeAsync(string resourceUriString)
        {
            //unsubscribe from resource
            if (container.ContainsKey(resourceUriString))
            {
                if (ephemeralObservers.ContainsKey(container[resourceUriString].Item1))
                {
                    await GraphManager.RemoveSubscriptionObserverAsync(container[resourceUriString].Item1, container[resourceUriString].Item2);
                    await GraphManager.UnsubscribeAsync(container[resourceUriString].Item1);
                    ephemeralObservers.Remove(container[resourceUriString].Item1);
                }

                container.Remove(resourceUriString);
            }
        }

        #region Dispose
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (leaseTimer != null)
                    {
                        leaseTimer.Stop();
                        leaseTimer.Dispose();
                    }

                    RemoveDurableObservers();
                    RemoveEphemeralObservers();
                }

                disposedValue = true;
            }
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion




        #region private methods
        private void Observer_OnNotify(object sender, MessageNotificationArgs e)
        {
            //signal the protocol adapter
            OnObserve?.Invoke(this, new ObserveMessageEventArgs(e.Message));
        }

        private void EnsureLeaseTimer()
        {
            if (leaseTimer == null)
            {
                leaseTimer = new System.Timers.Timer(30000);
                leaseTimer.Elapsed += LeaseTimer_Elapsed;
                leaseTimer.Start();
            }
        }

        private void LeaseTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {

            KeyValuePair<string, Tuple<string, string>>[] kvps = container.ToArray();

            if(kvps == null || kvps.Length == 0)
            {
                Task logTask = Log.LogInfoAsync("Lease container is empty.");
                Task.WhenAll(logTask);
                leaseTimer.Stop();
                return;
            }

            Task leaseTask = Task.Factory.StartNew(async () =>
            {
                if (kvps != null && kvps.Length > 0)
                {
                    foreach (var kvp in kvps)
                    {
                        await GraphManager.RenewObserverLeaseAsync(kvp.Value.Item1, kvp.Value.Item2, TimeSpan.FromSeconds(60.0));
                        //SubscriptionMetadata metadata = await GraphManager.GetSubscriptionMetadataAsync(kvp.Value.Item1);                      

                        //if (metadata != null)
                        //{
                            
                        //    //if(renewed)
                        //    //{
                        //    //    await Log.LogInfoAsync("Observer lease renewed.");
                        //    //}
                        //    //else
                        //    //{
                        //    //    await Log.LogWarningAsync("Observer lease could not be renewed.");
                        //    //}
                        //}
                        //else
                        //{
                        //    await Log.LogWarningAsync("Subscription metadata not found.");
                        //}
                    }
                }
            });

            Task.WhenAll(leaseTask);
            
        }

        private void RemoveDurableObservers()
        {            
            List<string> list = new List<string>();

            if (durableObservers.Count > 0)
            {
                List<Task> taskList = new List<Task>();
                KeyValuePair<string, IMessageObserver>[] kvps = durableObservers.ToArray();
                foreach (var item in kvps)
                {
                    IEnumerable<KeyValuePair<string, Tuple<string, string>>> items = container.Where((c) => c.Value.Item1 == item.Key);
                    foreach (var lease in items)
                    {
                        list.Add(lease.Value.Item1);

                        if (durableObservers.ContainsKey(lease.Value.Item1))
                        {
                            Task task = GraphManager.RemoveSubscriptionObserverAsync(lease.Value.Item1, lease.Value.Item2);
                            taskList.Add(task);
                        }
                    }
                }

                if (taskList.Count > 0)
                {
                    Task.WhenAll(taskList);
                }

                durableObservers.Clear();
                RemoveFromContainer(list);
            }
        }

        private void RemoveEphemeralObservers()
        {
            List<string> list = new List<string>();

            if (ephemeralObservers.Count > 0)
            {
                KeyValuePair<string, IMessageObserver>[] kvps = ephemeralObservers.ToArray();                
                List<Task> unobserveTaskList = new List<Task>();
                foreach (var item in kvps)
                {                    
                    IEnumerable<KeyValuePair<string, Tuple<string, string>>> items = container.Where((c) => c.Value.Item1 == item.Key);

                    foreach (var lease in items)
                    {
                        list.Add(lease.Value.Item1);
                        if (ephemeralObservers.ContainsKey(lease.Value.Item1))
                        {
                            Task unobserveTask = GraphManager.RemoveSubscriptionObserverAsync(lease.Value.Item1, lease.Value.Item2);
                            unobserveTaskList.Add(unobserveTask);
                        }
                    }
                }

                if (unobserveTaskList.Count > 0)
                {
                    Task.WhenAll(unobserveTaskList);                   
                }
                

                ephemeralObservers.Clear();
                RemoveFromContainer(list);
            }
        }

        private void RemoveFromContainer(string subscriptionUriString)
        {            
            List<string> list = new List<string>();
            var query = container.Where((c) => c.Value.Item1 == subscriptionUriString);

            foreach (var item in query)
            {
                list.Add(item.Key);
            }

            foreach (string item in list)
            {
                container.Remove(item);
            }
        }

        private void RemoveFromContainer(List<string> subscriptionUriStrings)
        {
            foreach (var item in subscriptionUriStrings)
            {
                RemoveFromContainer(item);
            }
        }

        private async Task AuditAsync(AuditRecord record = null)
        {   
            if (record != null && auditor.CanAudit)
            {
                await auditor.WriteAuditRecordAsync(record);
            }
        }

        private async Task SetAuditorAsync()
        {
            string connectionstring = await GraphManager.GetAuditConfigConnectionstringAsync();
            string tablename = await GraphManager.GetAuditConfigTablenameAsync();
            auditor = new Auditor(connectionstring, tablename);
        }
        #endregion
    }
}
