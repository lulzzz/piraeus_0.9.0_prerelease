using System;
using System.Configuration;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;
using WebGateway.Security;

namespace WebGateway
{
    public class WebApiApplication : System.Web.HttpApplication
    {
        protected void Application_Start()
        {
            // Catch unobserved exceptions from threads before they cause IIS to crash:
            TaskScheduler.UnobservedTaskException += (object sender, UnobservedTaskExceptionEventArgs excArgs) =>
            {
                TraceSource trace = new TraceSource("UnhandledExceptionTrace");       
                trace.TraceData(TraceEventType.Error, 1, excArgs.Exception);
                excArgs.SetObserved();
            };

            AppDomain.CurrentDomain.UnhandledException += (sender, args) => {
                Trace.TraceWarning("Web Gateway Unhandled exception");
                Trace.TraceWarning("Is Terminating {0}", args.IsTerminating);
                Exception ex = (Exception)args.ExceptionObject;
                Trace.TraceError(ex.Message);
                Trace.TraceError(ex.StackTrace);
            };

           

            AreaRegistration.RegisterAllAreas();
            GlobalConfiguration.Configure(WebApiConfig.Register);
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);

            // Create a trace listener for Web forms.
            WebPageTraceListener gbTraceListener = new WebPageTraceListener();
            // Add the event log trace listener to the collection.
            System.Diagnostics.Trace.Listeners.Add(gbTraceListener);

            if (!Orleans.GrainClient.IsInitialized)
            {
                bool dockerized = Convert.ToBoolean(ConfigurationManager.AppSettings["dockerize"]);
                if (!dockerized)
                {
                    OrleansClientConfig.TryStart("global.asax");
                }
                else
                {
                    OrleansClientConfig.TryStart("global.asax", System.Environment.GetEnvironmentVariable("GATEWAY_ORLEANS_SILO_DNS_HOSTNAME"));
                }

                Task task = ServiceIdentityConfig.Configure();
                Task.WhenAll(task);
            }

            
            


        }

      
    }
}
