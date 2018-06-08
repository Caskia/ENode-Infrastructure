using System;

namespace ENode.Kafka
{
    [Serializable]
    public class CommandMessage
    {
        public string CommandData { get; set; }
        public string ReplyAddress { get; set; }
    }
}