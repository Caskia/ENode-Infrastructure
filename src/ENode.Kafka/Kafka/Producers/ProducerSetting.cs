using System.Collections.Generic;
using System.Net;

namespace ENode.Kafka.Producers
{
    public class ProducerSetting
    {
        public IList<IPEndPoint> BrokerEndPoints { get; set; } = new List<IPEndPoint>();
    }
}