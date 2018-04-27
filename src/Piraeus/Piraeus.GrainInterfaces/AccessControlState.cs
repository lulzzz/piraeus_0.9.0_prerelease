using System;
using Capl.Authorization;
using Orleans.Concurrency;

namespace Piraeus.GrainInterfaces
{
    [Serializable]
 
    public class AccessControlState
    {

        public AuthorizationPolicy Policy { get; set; }
    }
}
