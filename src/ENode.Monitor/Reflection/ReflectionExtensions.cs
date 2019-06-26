using System.Reflection;

namespace ENode.Monitor.Reflection
{
    public static class ReflectionExtensions
    {
        public static T GetFieldValue<T>(this object obj, string field)
              where T : class
        {
            return obj.GetType()
                .GetField(field, BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(obj) as T;
        }
    }
}