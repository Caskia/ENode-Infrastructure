using System.Net;

namespace ENode.Diagnostics.Configurations
{
    public interface ICounterConfiguration
    {
        IPEndPoint Address { get; }

        string IP { get; set; }

        string Name { get; set; }

        int Port { get; set; }
    }
}