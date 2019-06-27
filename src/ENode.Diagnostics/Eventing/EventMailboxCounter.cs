using ENode.Diagnostics.Reflection;
using ENode.Eventing;
using ENode.Eventing.Impl;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace ENode.Diagnostics.Eventing
{
    public class EventMailboxCounter : IEventMailboxCounter
    {
        private readonly IEventService _eventService;

        public EventMailboxCounter(IEventService eventService)
        {
            _eventService = eventService;
        }

        private ConcurrentDictionary<string, EventMailBox> ProcessingEventMailboxes
        {
            get
            {
                return _eventService.GetFieldValue<ConcurrentDictionary<string, EventMailBox>>("_mailboxDict");
            }
        }

        public List<(string aggregateId, long unConsumedMessageCount)> GetProcessingMailboxesInfo(int limit = 10)
        {
            return GetProcessingMailboxes(limit)
                .Select(m => (m.AggregateRootId, Convert.ToInt64(m.GetFieldValue<ConcurrentQueue<EventCommittingContext>>("_messageQueue").Count)))
                .ToList();
        }

        private List<EventMailBox> GetProcessingMailboxes(int limit)
        {
            if (limit > 50)
            {
                throw new ArgumentException("can not greater than 50.", nameof(limit));
            }

            return ProcessingEventMailboxes
               .Select(m => m.Value)
               .OrderByDescending(m => m.GetFieldValue<ConcurrentQueue<EventCommittingContext>>("_messageQueue").Count)
               .Take(limit)
               .ToList();
        }
    }
}