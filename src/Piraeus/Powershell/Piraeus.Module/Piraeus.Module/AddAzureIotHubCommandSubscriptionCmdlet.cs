using Piraeus.Core.Metadata;
using System;
using System.Management.Automation;

namespace Piraeus.Module
{
    [Cmdlet(VerbsCommon.Add, "PiraeusIotHubCommandSubscription")]
    public class AddAzureIotHubCommandSubscriptionCmdlet : Cmdlet
    {
        [Parameter(HelpMessage = "Url of the service.", Mandatory = true)]
        public string ServiceUrl;

        [Parameter(HelpMessage = "Security token used to access the REST service.", Mandatory = true)]
        public string SecurityToken;

        [Parameter(HelpMessage = "Unique URI identifier of resource to subscribe.", Mandatory = true)]
        public string ResourceUriString;

        [Parameter(HelpMessage = "Host name of IoT Hub, e.g, <host>.azure-devices.net", Mandatory = true)]
        public string Host;

        [Parameter(HelpMessage = "Device ID that you will send messages.", Mandatory = true)]
        public string DeviceId;
        
        [Parameter(HelpMessage = "Name key used for authentication.", Mandatory = true)]
        public string KeyName;

        [Parameter(HelpMessage = "(Optional) property name to use when sending to device, i.e., used with property value.", Mandatory = false)]
        public string PropertyName;

        [Parameter(HelpMessage = "(Optional) property value to use when sending to device, i.e., used with property name.", Mandatory = false)]
        public string PropertyValue;

        [Parameter(HelpMessage = "SAS token used for authentication.", Mandatory = true)]
        public string Key;

        [Parameter(HelpMessage = "Number of blob storage clients to use.", Mandatory = false)]
        public int NumClients;

        [Parameter(HelpMessage = "Number of milliseconds to delay next write.", Mandatory = false)]
        public int Delay;

        [Parameter(HelpMessage = "Description of the subscription.", Mandatory = false)]
        public string Description;


        protected override void ProcessRecord()
        {
            string uriString = String.Format("iothub://{0}.azure-devices.net?deviceid={1}&keyname={2}", Host, DeviceId, KeyName);

            if(!String.IsNullOrEmpty(PropertyName))
            {
                uriString = String.Format("{0}&propname={1}&propvalue={1}", uriString, PropertyName, PropertyValue);
            }

            SubscriptionMetadata metadata = new SubscriptionMetadata()
            {
                IsEphemeral = false,
                NotifyAddress = uriString,
                SymmetricKey = Key,
                Description = this.Description
            };

            string url = String.Format("{0}/api2/resource/subscribe?resourceuristring={1}", ServiceUrl, ResourceUriString);
            RestRequestBuilder builder = new RestRequestBuilder("POST", url, RestConstants.ContentType.Json, false, SecurityToken);
            RestRequest request = new RestRequest(builder);

            string subscriptionUriString = request.Post<SubscriptionMetadata, string>(metadata);

            WriteObject(subscriptionUriString);
        }
    }
}
