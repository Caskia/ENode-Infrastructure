﻿using System.Threading.Tasks;

namespace ENode.Eventing
{
    /// <summary>Represents a processor to process event.
    /// </summary>
    public interface IProcessingEventProcessor
    {
        /// <summary>Process the given processingEvent.
        /// </summary>
        /// <param name="processingEvent"></param>
        Task ProcessAsync(ProcessingEvent processingEvent);

        /// <summary>Start the processor.
        /// </summary>
        void Start();

        /// <summary>Stop the processor.
        /// </summary>
        void Stop();
    }
}