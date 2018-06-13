namespace ENode.Kafka.Consumers
{
    public class TopicPartitionProcessQueue<TKey, TValue> : ProcessQueue<TKey, TValue>
    {
        #region Public Properites

        public int Partition { get; private set; }

        public string Topic { get; private set; }

        #endregion Public Properites

        public TopicPartitionProcessQueue(string topic, int partition)
        {
            Topic = topic;
            Partition = partition;
        }
    }
}