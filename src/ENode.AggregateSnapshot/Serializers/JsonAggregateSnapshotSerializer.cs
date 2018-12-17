using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;

namespace ENode.AggregateSnapshot.Serializers
{
    public class JsonAggregateSnapshotSerializer : IAggregateSnapshotSerializer
    {
        public JsonAggregateSnapshotSerializer()
        {
            Settings = new JsonSerializerSettings
            {
                Converters = new List<JsonConverter> { new IsoDateTimeConverter() },
                ContractResolver = new CustomContractResolver()
            };
        }

        public JsonSerializerSettings Settings { get; private set; }

        /// <summary>Deserialize a json string to an object.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public object Deserialize(string value, Type type)
        {
            return JsonConvert.DeserializeObject(value, type, Settings);
        }

        /// <summary>Deserialize a json string to a strong type object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public T Deserialize<T>(string value) where T : class
        {
            return JsonConvert.DeserializeObject<T>(JObject.Parse(value).ToString(), Settings);
        }

        /// <summary>Serialize an object to json string.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public string Serialize(object obj)
        {
            return obj == null ? null : JsonConvert.SerializeObject(obj, Settings);
        }

        private class CustomContractResolver : DefaultContractResolver
        {
            private List<string> _ignoredPropertyNames = new List<string>()
            {
                "_uncommittedEvents"
            };

            //protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
            //{
            //    var jsonProperty = base.CreateProperty(member, memberSerialization);
            //    if (!jsonProperty.Writable)
            //    {
            //        var property = member as PropertyInfo;
            //        if (property != null)
            //        {
            //            var hasPrivateSetter = property.GetSetMethod(true) != null;
            //            jsonProperty.Writable = hasPrivateSetter;
            //        }
            //    }

            //    return jsonProperty;
            //}

            protected override List<MemberInfo> GetSerializableMembers(Type objectType)
            {
                var members = base.GetSerializableMembers(objectType);

                return members
                      .Select(m => m as PropertyInfo)
                      .Where(p => p != null && p.GetSetMethod(true) != null && p.GetGetMethod(true) != null && !_ignoredPropertyNames.Contains(p.Name))
                      .Select(p => p as MemberInfo)
                      .ToList();
            }
        }
    }
}