using System.Collections.Generic;
using System.Net;

namespace ENode.Kafka.Consumers
{
    public class ConsumerSetting
    {
        public ConsumerSetting()
        {
            CommitConsumerOffsetInterval = 1000;
            ConsumeFlowControlStepPercent = 1;
            ConsumeFlowControlStepWaitMilliseconds = 1;
            ConsumeFlowControlThreshold = 1000;
            GroupName = "DefaultGroup";
            MessageHandleMode = MessageHandleMode.Parallel;
            RetryMessageInterval = 1000;
        }

        public IList<IPEndPoint> BrokerEndPoints { get; set; } = new List<IPEndPoint>();

        public int CommitConsumerOffsetInterval { get; set; }

        /// <summary>
        /// 当拉取消息开始流控时，需要逐渐增加流控时间的步长百分比，默认为1%；
        /// <remarks>
        /// 假设当前本地拉取且并未消费的消息数超过阀值时，需要逐渐增加流控时间；具体增加多少时间取决于
        /// PullMessageFlowControlStepPercent以及PullMessageFlowControlStepWaitMilliseconds属性的配置值；
        /// 举个例子，假设流控阀值为1000，步长百分比为1%，每个步长等待时间为1ms；
        /// 然后，假如当前拉取到本地未消费的消息数为1200，
        /// 则超出阀值的消息数是：1200 - 1000 = 200，
        /// 步长为：1000 * 1% = 10；
        /// 然后，200 / 10 = 20，即当前超出的消息数是步长的20倍；
        /// 所以，最后需要等待的时间为20 * 1ms = 20ms;
        /// </remarks>
        /// </summary>
        public int ConsumeFlowControlStepPercent { get; set; }

        /// <summary>
        /// 当拉取消息开始流控时，每个步长需要等待的时间，默认为1ms；
        /// </summary>
        public int ConsumeFlowControlStepWaitMilliseconds { get; set; }

        /// <summary>
        /// 拉取消息时，开始流控的阀值，默认为1000；即当前拉取到本地未消费的消息数到达1000时，将开始做流控，减慢拉取速度；
        /// </summary>
        public int ConsumeFlowControlThreshold { get; set; }

        public string GroupName { get; set; }

        public MessageHandleMode MessageHandleMode { get; set; }
        public int RetryMessageInterval { get; set; }
    }
}