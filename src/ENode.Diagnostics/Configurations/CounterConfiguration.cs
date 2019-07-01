using ENode.Diagnostics.Utils;
using System.Net;

namespace ENode.Diagnostics.Configurations
{
    public class CounterConfiguration : ICounterConfiguration
    {
        public IPEndPoint Address
        {
            get
            {
                if (string.IsNullOrEmpty(IP))
                {
                    return new IPEndPoint(SocketUtils.GetLocalIPV4(), Port);
                }

                return new IPEndPoint(IPAddress.Parse(IP), Port);
            }
        }

        public string IP { get; set; }

        public string Name { get; set; }

        public int Port { get; set; }
    }
}