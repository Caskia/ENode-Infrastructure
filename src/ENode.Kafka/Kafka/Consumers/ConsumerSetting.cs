using System.Collections.Generic;
using System.Net;

namespace ENode.Kafka.Consumers
{
    public class ConsumerSetting
    {
        public ConsumerSetting()
        {
            GroupName = "DefaultGroup";
        }

        public IList<IPEndPoint> BrokerEndPoints { get; set; } = new List<IPEndPoint>();

        public string GroupName { get; set; }
    }
}