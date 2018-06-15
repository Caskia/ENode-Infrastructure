using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace ENode.Kafka.Utils
{
    public class ObjectUtils
    {
        public static object DeserializeFromStream(MemoryStream stream)
        {
            var formatter = new BinaryFormatter();
            stream.Seek(0, SeekOrigin.Begin);
            var o = formatter.Deserialize(stream);
            return o;
        }

        public static MemoryStream SerializeToStream(object o)
        {
            var stream = new MemoryStream();
            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, o);
            return stream;
        }
    }
}