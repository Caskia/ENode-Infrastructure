using ENode.Eventing;
using ENode.Eventing.Impl;
using ENode.Monitor.Reflection;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace ENode.Monitor.Eventing
{
    public class EventMailboxMonitor : IEventMailboxMonitor
    {
        private readonly IEventService _eventService;

        public EventMailboxMonitor(IEventService eventService)
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
                throw new System.ArgumentException("can not greater than 50.", nameof(limit));
            }

            return ProcessingEventMailboxes
               .Select(m => m.Value)
               .OrderByDescending(m => m.GetFieldValue<ConcurrentQueue<EventCommittingContext>>("_messageQueue").Count)
               .Take(limit)
               .ToList();
        }
    }
}