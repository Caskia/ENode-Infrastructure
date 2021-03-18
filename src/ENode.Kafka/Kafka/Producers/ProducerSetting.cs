using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace ENode.Kafka.Producers
{
    public class ProducerSetting
    {
        public string BootstrapServers
        {
            get
            {
                return string.Join(",", BrokerEndPoints.Select(e => e.Address.ToString() + ":" + e.Port));
            }
        }

        public IList<IPEndPoint> BrokerEndPoints { get; set; } = new List<IPEndPoint>();
    }
}