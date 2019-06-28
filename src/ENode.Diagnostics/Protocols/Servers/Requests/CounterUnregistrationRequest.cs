using System;

namespace ENode.Diagnostics.Protocols.Servers.Requests
{
    [Serializable]
    public class CounterUnregistrationRequest
    {
        public int ProcessId { get; set; }

        public string ProcessName { get; set; }
    }
}