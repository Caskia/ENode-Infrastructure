using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using ENode.Domain;

namespace ENode.AggregateSnapshot.Serializers
{
    public class JsonAggregateSnapshotSerializer : IAggregateSnapshotSerializer
    {
        public JsonAggregateSnapshotSerializer()
        {
            Settings = new JsonSerializerSettings
            {
                Converters = new List<JsonConverter> { new IsoDateTimeConverter() },
                ContractResolver = new CustomContractResolver(),
                ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
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
            protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
            {
                if (typeof(IAggregateRoot).IsAssignableFrom(type))
                {
                    var properties = type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                           .Select(p => base.CreateProperty(p, memberSerialization))
                           .ToList();
                    properties.ForEach(p => { p.Writable = true; p.Readable = true; });
                    return properties;
                }
                else
                {
                    return base.CreateProperties(type, memberSerialization);
                }
            }
        }
    }
}