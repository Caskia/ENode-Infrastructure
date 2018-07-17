﻿using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class DeleteTopicForClusterRequest
    {
        public string ClusterName { get; set; }
        public string Topic { get; set; }
    }
}
