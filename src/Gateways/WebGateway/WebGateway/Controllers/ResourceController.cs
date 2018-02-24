using Piraeus.Core.Messaging;
using Piraeus.Core.Metadata;
using Piraeus.Grains;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using WebGateway.Security;

namespace WebGateway.Controllers
{
    public class ResourceController : ApiController
    {
        public ResourceController()
        {
            if (!Orleans.GrainClient.IsInitialized)
            {
                bool dockerized = Convert.ToBoolean(ConfigurationManager.AppSettings["dockerize"]);
                if (!dockerized)
                {
                    OrleansClientConfig.TryStart("ManageController");
                }
                else
                {
                    OrleansClientConfig.TryStart("ManageController", System.Environment.GetEnvironmentVariable("GATEWAY_ORLEANS_SILO_DNS_HOSTNAME"));
                }

                Task task = ServiceIdentityConfig.Configure();
                Task.WhenAll(task);
            }

            Trace.TraceInformation("Orleans grain client initialized {0} is resource controller", Orleans.GrainClient.IsInitialized);

        }

        //[CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetResourceList()
        {
            try
            {
                IEnumerable<string> list = await GraphManager.GetResourceListAsync();
                return Request.CreateResponse<IEnumerable<string>>(HttpStatusCode.OK, list);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }


        //[CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetResourceMetadata(string resourceUriString)
        {
            try
            {
                ResourceMetadata metadata = await GraphManager.GetResourceMetadataAsync(resourceUriString);
                return Request.CreateResponse<ResourceMetadata>(HttpStatusCode.OK, metadata);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("Failed to get resource metadata.");
                Trace.TraceError(ex.Message);
                Trace.TraceError(ex.StackTrace);
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        [HttpGet]
        public async Task<HttpResponseMessage> GetResourceMetrics(string resourceUriString)
        {
            try
            {
                CommunicationMetrics metrics = await GraphManager.GetResourceMetricsAsync(resourceUriString);
                return Request.CreateResponse<CommunicationMetrics>(HttpStatusCode.OK, metrics);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        //[CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpPut]
        public async Task<HttpResponseMessage> UpsertResourceMetadata(ResourceMetadata metadata)
        {
            try
            {

                await GraphManager.UpsertResourceMetadataAsync(metadata);
                return new HttpResponseMessage(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("Failed to upsert resource metadata.");
                Trace.TraceError(ex.Message);
                Trace.TraceError(ex.StackTrace);
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        //[CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpPost]
        public async Task<HttpResponseMessage> Subscribe(string resourceUriString, SubscriptionMetadata metadata)
        {
            try
            {
                string subscriptionUriString = await GraphManager.SubscribeAsync(resourceUriString, metadata);
                return Request.CreateResponse<string>(HttpStatusCode.OK, subscriptionUriString);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        //[CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetResourceSubscriptionList(string resourceUriString)
        {
            try
            {
                IEnumerable<string> list = await GraphManager.GetResourceSubscriptionListAsync(resourceUriString);
                return Request.CreateResponse(HttpStatusCode.OK, list);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        //[CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpDelete]
        public async Task<HttpResponseMessage> Unsubscribe(string subscriptionUriString)
        {
            try
            {
                await GraphManager.UnsubscribeAsync(subscriptionUriString.ToLowerInvariant());
                return new HttpResponseMessage(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }

        //[CaplAuthorize(PolicyId = "http://www.skunklab.io/api/management")]
        [HttpDelete]
        public async Task<HttpResponseMessage> DeleteResource(string resourceUriString)
        {
            try
            {
                await GraphManager.ClearResourceAsync(resourceUriString);
                return new HttpResponseMessage(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
            }
        }
    }
}
