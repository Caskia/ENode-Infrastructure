using ECommon.Utilities;
using System;
using System.Text;

namespace ENode.Kafka
{
    public class ENodeMessage
    {
        public ENodeMessage()
        {
        }

        public ENodeMessage(string topic, int code, string body, string tag = null) : this(topic, code, body, DateTime.Now, tag)
        {
        }

        public ENodeMessage(string topic, int code, string body, DateTime createdTime, string tag = null)
        {
            Ensure.NotNull(topic, "topic");
            Ensure.Positive(code, "code");
            Ensure.NotNull(body, "body");
            Topic = topic;
            Tag = tag;
            Code = code;
            Body = body;
            CreatedTime = createdTime;
        }

        public string Body { get; set; }

        public int Code { get; set; }

        public DateTime CreatedTime { get; set; }

        public string Tag { get; set; }

        public string Topic { get; set; }

        public override string ToString()
        {
            return string.Format("[Topic={0},Code={1},Tag={2},CreatedTime={3},BodyLength={4}]", Topic, Code, Tag, CreatedTime, Encoding.UTF8.GetBytes(Body).Length);
        }
    }
}