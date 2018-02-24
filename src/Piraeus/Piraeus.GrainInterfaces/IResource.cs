using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Piraeus.Core.Messaging;
using Piraeus.Core.Metadata;

namespace Piraeus.GrainInterfaces
{
    public interface IResource : IGrainWithStringKey
    {
        Task UpsertMetadataAsync(ResourceMetadata metadata);

        Task<CommunicationMetrics> GetMetricsAsync();
        Task<ResourceMetadata> GetMetadataAsync();

        Task SubscribeAsync(ISubscription subscription);

        Task UnsubscribeAsync(string subscriptionUriString);

        Task UnsubscribeAsync(string subscriptionUriString, string identity);

        Task<IEnumerable<string>> GetSubscriptionListAsync();

        Task PublishAsync(EventMessage message);

        Task PublishAsync(EventMessage message, List<KeyValuePair<string, string>> indexes);

        Task ClearAsync();

        Task<string> AddObserverAsync(TimeSpan lifetime, IMetricObserver observer);

        Task<string> AddObserverAsync(TimeSpan lifetime, IErrorObserver observer);

        Task RemoveObserverAsync(string leaseKey);

        Task<bool> RenewObserverLeaseAsync(string leaseKey, TimeSpan lifetime);

    }
}
