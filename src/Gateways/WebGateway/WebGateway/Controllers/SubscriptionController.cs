using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Piraeus.Core.Messaging;
using Piraeus.Core.Metadata;
using Piraeus.Grains;
using WebGateway.Security;

namespace WebGateway.Controllers
{
    public class SubscriptionController : ApiController
    {
        public SubscriptionController()
        {
            if (!Orleans.GrainClient.IsInitialized)
            {
                bool dockerized = Convert.ToBoolean(ConfigurationManager.AppSettings["dockerize"]);
                if (!dockerized)
                {
                    OrleansClientConfig.TryStart("SubscriptionController");
                }
                else
                {
                    OrleansClientConfig.TryStart("SubscriptionController", System.Environment.GetEnvironmentVariable("GATEWAY_ORLEANS_SILO_DNS_HOSTNAME"));
                }

                Task task = ServiceIdentityConfig.Configure();
                Task.WhenAll(task);
            }

        }

        [CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetSubscriptionMetadata(string subscriptionUriString)
        {
            try
            {
                SubscriptionMetadata metadata = await GraphManager.GetSubscriptionMetadataAsync(subscriptionUriString);
                return Request.CreateResponse<SubscriptionMetadata>(HttpStatusCode.OK, metadata);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        [CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetSubscriptionMetrics(string subscriptionUriString)
        {
            try
            {
                CommunicationMetrics metrics = await GraphManager.GetSubscriptionMetricsAsync(subscriptionUriString);
                return Request.CreateResponse<CommunicationMetrics>(HttpStatusCode.OK, metrics);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        [CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpPut]
        public async Task<HttpResponseMessage> UpsertSubscriptionMetadata(SubscriptionMetadata metadata)
        {
            try
            {
                await GraphManager.UpsertSubscriptionMetadataAsync(metadata);
                return new HttpResponseMessage(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        [CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetSubscriberSubscriptions(string identity)
        {
            try
            {
                IEnumerable<string> list = await GraphManager.GetSubscriberSubscriptionsListAsync(identity);
                return Request.CreateResponse<IEnumerable<string>>(HttpStatusCode.OK, list);

            }
            catch(Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }


        
    }
}
