﻿using System;
using System.Collections.Generic;

namespace ENode.Commanding
{
    /// <summary>Represents a command result.
    /// </summary>
    [Serializable]
    public class CommandResult
    {
        /// <summary>Represents the result status of the command.
        /// </summary>
        public CommandStatus Status { get; private set; }
        /// <summary>Represents the unique identifier of the command.
        /// </summary>
        public string CommandId { get; private set; }
        /// <summary>Represents the aggregate root id associated with the command.
        /// </summary>
        public string AggregateRootId { get; private set; }
        /// <summary>Represents the command result data.
        /// </summary>
        public string Result { get; private set; }
        /// <summary>Represents the command result data type.
        /// </summary>
        public string ResultType { get; private set; }

        /// <summary>Default constructor.
        /// </summary>
        public CommandResult() { }
        /// <summary>Parameterized constructor.
        /// </summary>
        public CommandResult(CommandStatus status, string commandId, string aggregateRootId, string result = null, string resultType = null)
        {
            Status = status;
            CommandId = commandId;
            AggregateRootId = aggregateRootId;
            Result = result;
            ResultType = resultType;
        }

        /// <summary>Overrides to return the command result info.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("[CommandId={0},Status={1},AggregateRootId={2},Result={3},ResultType={4}]",
                CommandId,
                Status,
                AggregateRootId,
                Result,
                ResultType);
        }
    }
    /// <summary>Represents the command result status enum.
    /// </summary>
    public enum CommandStatus
    {
        None = 0,
        Success = 1,
        NothingChanged = 2,
        Failed = 3
    }
}
