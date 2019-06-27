using System.Reflection;
using System;

namespace ENode.Diagnostics.Reflection
{
    public static class ReflectionExtensions
    {
        public static T GetFieldValue<T>(this object obj, string field)
              where T : class
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            return obj.GetType()
                .GetField(field, BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(obj) as T;
        }
    }
}