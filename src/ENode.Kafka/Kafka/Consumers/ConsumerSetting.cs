using System.Collections.Generic;
using System.Net;

namespace ENode.Kafka.Consumers
{
    public class ConsumerSetting
    {
        public ConsumerSetting()
        {
            GroupName = "DefaultGroup";
            MessageHandleMode = MessageHandleMode.Parallel;
            RetryMessageInterval = 1000;
        }

        public IList<IPEndPoint> BrokerEndPoints { get; set; } = new List<IPEndPoint>();

        public string GroupName { get; set; }

        public MessageHandleMode MessageHandleMode { get; set; }

        public int RetryMessageInterval { get; set; }
    }
}