using System;

namespace ENode.Diagnostics.Protocols.Servers.Requests
{
    [Serializable]
    public class CounterRegistrationRequest
    {
        public int ProcessId { get; set; }

        public string ProcessName { get; set; }

        public int ServicePort { get; set; }
    }
}