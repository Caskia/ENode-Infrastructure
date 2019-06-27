using ENode.Eventing;
using System;

namespace ENode.Diagnostics.Tests.Domain
{
    [Serializable]
    public class BookTestEvent : DomainEvent<string>
    {
        public string Name { get; set; }
    }
}