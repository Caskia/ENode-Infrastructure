using ECommon.Utilities;
using System;

namespace ENode.Kafka
{
    public class EnodeMessage
    {
        public EnodeMessage()
        {
        }

        public EnodeMessage(string topic, int code, byte[] body, string tag = null) : this(topic, code, body, DateTime.Now, tag)
        {
        }

        public EnodeMessage(string topic, int code, byte[] body, DateTime createdTime, string tag = null)
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

        public byte[] Body { get; set; }

        public int Code { get; set; }

        public DateTime CreatedTime { get; set; }

        public string Tag { get; set; }

        public string Topic { get; set; }

        public override string ToString()
        {
            return string.Format("[Topic={0},Code={1},Tag={2},CreatedTime={3},BodyLength={4}]", Topic, Code, Tag, CreatedTime, Body.Length);
        }
    }
}