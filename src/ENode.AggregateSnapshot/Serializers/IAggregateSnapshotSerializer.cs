using System;

namespace ENode.AggregateSnapshot.Serializers
{
    public interface IAggregateSnapshotSerializer
    {
        object Deserialize(string value, Type type);

        T Deserialize<T>(string value) where T : class;

        string Serialize(object obj);
    }
}