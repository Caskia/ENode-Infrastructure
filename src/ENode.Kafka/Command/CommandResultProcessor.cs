﻿using DotNetty.Codecs;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using ENode.Commanding;
using ENode.Kafka.Netty;
using ENode.Kafka.Netty.Codecs;
using ENode.Kafka.Utils;
using System;
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class CommandResultProcessor
    {
        private BlockingCollection<CommandResult> _commandExecutedMessageLocalQueue;
        private Worker _commandExecutedMessageWorker;
        private ConcurrentDictionary<string, CommandTaskCompletionSource> _commandTaskDict;
        private BlockingCollection<DomainEventHandledMessage> _domainEventHandledMessageLocalQueue;
        private Worker _domainEventHandledMessageWorker;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private NettyServer _server;
        private bool _started;
        public IPEndPoint BindingAddress { get; private set; }
        public string BindingHostname { get; private set; }

        public string BindingServerAddress
        {
            get
            {
                if (!string.IsNullOrEmpty(BindingHostname))
                {
                    return BindingHostname;
                }
                return BindingAddress.ToString();
            }
        }

        public void HandleRequest(Request remotingRequest)
        {
            if (remotingRequest.Code == (int)CommandReturnType.CommandExecuted)
            {
                var json = remotingRequest.Body.ToStringUtf8();
                var result = _jsonSerializer.Deserialize<CommandResult>(json);
                _commandExecutedMessageLocalQueue.Add(result);
            }
            else if (remotingRequest.Code == (int)CommandReturnType.EventHandled)
            {
                var json = remotingRequest.Body.ToStringUtf8();
                var message = _jsonSerializer.Deserialize<DomainEventHandledMessage>(json);
                _domainEventHandledMessageLocalQueue.Add(message);
            }
            else
            {
                _logger.ErrorFormat("Invalid remoting request code: {0}", remotingRequest.Code);
            }
        }

        public CommandResultProcessor Initialize(string hostname, int port)
        {
            var bindingAddress = SocketUtils.GetIPEndPointFromHostName(hostname, port);
            BindingHostname = $"{hostname}:{port}";
            return Initialize(bindingAddress);
        }

        public CommandResultProcessor Initialize(IPEndPoint bindingAddress)
        {
            var serverSetting = new NettyServerSetting(
                channel =>
                {
                    var pipeline = channel.Pipeline;

                    pipeline.AddLast(typeof(LengthFieldPrepender).Name, new LengthFieldPrepender(2));
                    pipeline.AddLast(typeof(LengthFieldBasedFrameDecoder).Name, new LengthFieldBasedFrameDecoder(ushort.MaxValue, 0, 2, 0, 2));
                    pipeline.AddLast(typeof(RequestEncoder).Name, new RequestEncoder());
                    pipeline.AddLast(typeof(RequestDecoder).Name, new RequestDecoder(Request.Parser));
                    pipeline.AddLast(typeof(CommandResultChannelHandler).Name, new CommandResultChannelHandler(this));
                }
            );
            _server = new NettyServer("CommandResultProcessor.RemotingServer", bindingAddress, serverSetting);

            _commandTaskDict = new ConcurrentDictionary<string, CommandTaskCompletionSource>();
            _commandExecutedMessageLocalQueue = new BlockingCollection<CommandResult>(new ConcurrentQueue<CommandResult>());
            _domainEventHandledMessageLocalQueue = new BlockingCollection<DomainEventHandledMessage>(new ConcurrentQueue<DomainEventHandledMessage>());
            _commandExecutedMessageWorker = new Worker("ProcessExecutedCommandMessage", () => ProcessExecutedCommandMessage(_commandExecutedMessageLocalQueue.Take()));
            _domainEventHandledMessageWorker = new Worker("ProcessDomainEventHandledMessage", () => ProcessDomainEventHandledMessage(_domainEventHandledMessageLocalQueue.Take()));
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            BindingAddress = bindingAddress;
            return this;
        }

        public void ProcessFailedSendingCommand(ICommand command)
        {
            if (_commandTaskDict.TryRemove(command.Id, out CommandTaskCompletionSource commandTaskCompletionSource))
            {
                var commandResult = new CommandResult(CommandStatus.Failed, command.Id, command.AggregateRootId, "Failed to send the command.", typeof(string).FullName);
                commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult);
            }
        }

        public void RegisterProcessingCommand(ICommand command, CommandReturnType commandReturnType, TaskCompletionSource<CommandResult> taskCompletionSource)
        {
            if (!_commandTaskDict.TryAdd(command.Id, new CommandTaskCompletionSource { CommandReturnType = commandReturnType, TaskCompletionSource = taskCompletionSource }))
            {
                throw new Exception(string.Format("Duplicate processing command registration, type:{0}, id:{1}", command.GetType().Name, command.Id));
            }
        }

        public CommandResultProcessor Shutdown()
        {
            _server.Shutdown();
            _commandExecutedMessageWorker.Stop();
            _domainEventHandledMessageWorker.Stop();
            return this;
        }

        public CommandResultProcessor Start()
        {
            if (_started) return this;

            _server.Start();
            _commandExecutedMessageWorker.Start();
            _domainEventHandledMessageWorker.Start();

            _started = true;

            _logger.InfoFormat("Command result processor started, bindingAddress: {0}", BindingAddress);

            return this;
        }

        private void ProcessDomainEventHandledMessage(DomainEventHandledMessage message)
        {
            if (_commandTaskDict.TryRemove(message.CommandId, out CommandTaskCompletionSource commandTaskCompletionSource))
            {
                var commandResult = new CommandResult(CommandStatus.Success, message.CommandId, message.AggregateRootId, message.CommandResult, message.CommandResult != null ? typeof(string).FullName : null);
                if (commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult))
                {
                    if (_logger.IsDebugEnabled)
                    {
                        _logger.DebugFormat("Command result return, {0}", commandResult);
                    }
                }
            }
        }

        private void ProcessExecutedCommandMessage(CommandResult commandResult)
        {
            if (_commandTaskDict.TryGetValue(commandResult.CommandId, out CommandTaskCompletionSource commandTaskCompletionSource))
            {
                if (commandTaskCompletionSource.CommandReturnType == CommandReturnType.CommandExecuted)
                {
                    _commandTaskDict.Remove(commandResult.CommandId);
                    if (commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult))
                    {
                        if (_logger.IsDebugEnabled)
                        {
                            _logger.DebugFormat("Command result return, {0}", commandResult);
                        }
                    }
                }
                else if (commandTaskCompletionSource.CommandReturnType == CommandReturnType.EventHandled)
                {
                    if (commandResult.Status == CommandStatus.Failed || commandResult.Status == CommandStatus.NothingChanged)
                    {
                        _commandTaskDict.Remove(commandResult.CommandId);
                        if (commandTaskCompletionSource.TaskCompletionSource.TrySetResult(commandResult))
                        {
                            if (_logger.IsDebugEnabled)
                            {
                                _logger.DebugFormat("Command result return, {0}", commandResult);
                            }
                        }
                    }
                }
            }
        }

        private class CommandTaskCompletionSource
        {
            public CommandReturnType CommandReturnType { get; set; }

            public TaskCompletionSource<CommandResult> TaskCompletionSource { get; set; }
        }
    }
}