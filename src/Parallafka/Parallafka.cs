using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public class Parallafka<TKey, TValue> : IParallafka<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;

        private readonly IParallafkaConfig<TKey, TValue> _config;

        private readonly CancellationTokenSource _handlerShutdownCts = new();

        private readonly CancellationTokenSource _controllerHardStopCts = new();

        /// <summary>
        /// A bounded buffer for messages polled from Kafka.
        /// </summary>
        private readonly ITargetBlock<IKafkaMessage<TKey, TValue>> _routingTarget;

        /// <summary>
        /// Messages eligible for handler threads to pick up and handle.
        /// </summary>
        private readonly ITargetBlock<IKafkaMessage<TKey, TValue>> _handlingTarget;

        /// <summary>
        /// This is where messages go after being handled: enqueued to be committed when it's safe.
        /// </summary>
        private readonly ITargetBlock<IKafkaMessage<TKey, TValue>> _committerTarget;

        /// <summary>
        /// Maps partition number to a queue of the messages received from that partition.
        /// The front of the queue is the earliest uncommitted message. Messages are removed once committed.
        /// It's safe to commit a handled message provided all earlier (lower-offset) messages have also been marked
        /// as handled.
        /// </summary>
        private readonly Dictionary<int, Queue<IKafkaMessage<TKey, TValue>>> _messagesNotYetCommittedByPartition;

        /// <summary>
        /// Maps the Kafka message key to a queue of as yet unhandled messages with the key, or null rather
        /// than a queue if no further messages with the key have arrived and started piling up.
        /// The presence of the key in this dictionary lets us track which messages are "handling in progress,"
        /// while the queue, if present, holds the next messages with the key to handle, in FIFO order, preserving
        /// Kafka's same-key order guarantee.
        /// </summary>
        private readonly Dictionary<TKey, Queue<IKafkaMessage<TKey, TValue>>> _messagesToHandleForKey;

        private readonly ConcurrentDictionary<int, OffsetStatus> _maxOffsetPickedUpForHandlingByPartition;

        /// <summary>
        /// The function that actually processes the message
        /// </summary>
        private readonly Func<IKafkaMessage<TKey, TValue>, Task> _messageHandlerAsync;

        private readonly ILogger _logger;

        public Parallafka(
            IKafkaConsumer<TKey, TValue> consumer,
            IParallafkaConfig<TKey, TValue> config,
            Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync)
        {
            this._consumer = consumer;
            this._config = config;
            this._logger = config.Logger;
            this._messageHandlerAsync = messageHandlerAsync;

            // TODO: Configurable caps, good defaults.
            // Are there any deadlocks or performance issues with these caps in general?
            this._routingTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(this.RouteMessages,
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 999,
                    MaxDegreeOfParallelism = 1
                });

            this._handlingTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(this.MessageHandler,
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = this._config.MaxDegreeOfParallelism
                });

            this._committerTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(this.CommitMessage,
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 333,
                    MaxDegreeOfParallelism = 1
                });
            
            this._messagesToHandleForKey = new();
            this._messagesNotYetCommittedByPartition = new();
            this._maxOffsetPickedUpForHandlingByPartition = new();
        }

        private async Task RouteMessages(IKafkaMessage<TKey, TValue> message)
        {
            if (!this._messagesNotYetCommittedByPartition.TryGetValue(message.Offset.Partition,
                out Queue<IKafkaMessage<TKey, TValue>> messagesInPartition))
            {
                messagesInPartition = new Queue<IKafkaMessage<TKey, TValue>>();
                this._messagesNotYetCommittedByPartition[message.Offset.Partition] = messagesInPartition;
            }
            messagesInPartition.Enqueue(message);

            bool aMessageWithThisKeyIsCurrentlyBeingHandled = this._messagesToHandleForKey.TryGetValue(message.Key,
                out Queue<IKafkaMessage<TKey, TValue>> messagesToHandleForKey);
            if (aMessageWithThisKeyIsCurrentlyBeingHandled)
            {
                if (messagesToHandleForKey == null)
                {
                    messagesToHandleForKey = new Queue<IKafkaMessage<TKey, TValue>>();
                    this._messagesToHandleForKey[message.Key] = messagesToHandleForKey;
                }

                messagesToHandleForKey.Enqueue(message);
            }
            else
            {
                // Add the key to indicate that a message with the key is being handled (see above)
                // so we know to queue up any additional messages with the key.
                // Without this line, FIFO same-key handling order is not enforced.
                // Remove it to test the tests.
                this._messagesToHandleForKey[message.Key] = null;
                await this._handlingTarget.SendAsync(message, this._handlerShutdownCts.Token);
            }

            // If there are any messages with the same key queued, make the next one available for handling.
            // TODO: Is this safe as far as commits?
            if (this._messagesToHandleForKey.TryGetValue(message.Key,
                out Queue<IKafkaMessage<TKey, TValue>> messagesQueuedForKey))
            {
                if (messagesQueuedForKey == null || messagesQueuedForKey.Count == 0)
                {
                    this._messagesToHandleForKey.Remove(message.Key);
                }
                else
                {
                    await this._handlingTarget.SendAsync(messagesQueuedForKey.Dequeue());
                }
            }
        }

        private async Task CommitMessage(IKafkaMessage<TKey, TValue> message)
        {
            Queue<IKafkaMessage<TKey, TValue>> messagesNotYetCommitted = this._messagesNotYetCommittedByPartition[message.Offset.Partition];

            if (messagesNotYetCommitted.Count > 0 && message == messagesNotYetCommitted.Peek())
            {
                var messagesToCommit = new List<IKafkaMessage<TKey, TValue>>();
                while (messagesNotYetCommitted.TryPeek(out IKafkaMessage<TKey, TValue> msg) && msg.WasHandled)
                {
                    messagesToCommit.Add(msg);
                    messagesNotYetCommitted.Dequeue();
                }

                while (!this._controllerHardStopCts.IsCancellationRequested && messagesToCommit.Count > 0)
                {
                    try
                    {
                        // TODO: Optimize commit frequency; make configurable.
                        // TODO: inject CancelToken for hard-stop strategy?
                        // Also, optimize by committing just the most recently handled msg in the partition,
                        // if possible, and update docs about the method's contract.
                        await this._consumer.CommitAsync(messagesToCommit.Select(m => m.Offset));

                        foreach (var committedMsg in messagesToCommit)
                        {
                            if (!this._maxOffsetPickedUpForHandlingByPartition.TryGetValue(
                                committedMsg.Offset.Partition, out OffsetStatus maxOffsetPickedUp))
                            {
                                this._logger.LogWarning("No max offset for partition {0}",
                                    committedMsg.Offset.Partition);
                                continue;
                            }

                            if (committedMsg.Offset.Offset == maxOffsetPickedUp.Offset.Offset)
                            {
                                maxOffsetPickedUp.IsCommitted = true;
                            }
                        }

                        break;
                    }
                    catch (Exception e)
                    {
                        this._logger.LogError(e, "Error committing offsets");
                        await Task.Delay(99);
                    }
                }
            }
        }

        public Task ConsumeAsync(CancellationToken stopToken)
        {
            return this.KafkaPollerThread(stopToken);
        }

        private async Task MessageHandler(IKafkaMessage<TKey, TValue> message)
        {
            try
            {
                await this._messageHandlerAsync(message);
            }
            catch (Exception e)
            {
                // TODO: User is responsible for handling errors but should we do anything else here?
                this._logger.LogError(e,
                    "Unhandled exception in handler callback. Warning: Still attempting to commit this and handle further messages. Partition={0}, Offset={1}",
                    message.Offset.Partition, message.Offset.Offset);
            }

            message.WasHandled = true;

            try
            {
                await _committerTarget.SendAsync(message, this._handlerShutdownCts.Token);
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task KafkaPollerThread(CancellationToken stopToken)
        {
            try
            {
                while (!stopToken.IsCancellationRequested)
                {
                    // TODO: Error handling
                    try
                    {
                        IKafkaMessage<TKey, TValue> message = await this._consumer.PollAsync(stopToken);
                        if (message == null)
                        {
                            if (!stopToken.IsCancellationRequested)
                            {
                                this._logger.LogWarning(
                                    "Polled a null message while not shutting down: breach of IKafkaConsumer contract");
                                await Task.Delay(50);
                            }
                        }
                        else
                        {
                            // TODO: Can this be done without handler thread contention, or at least with out-of-band sorts and purges with
                            // a long queue of recents?
                            // Maybe use non-concurrent dict so there's no contention except within partition.
                            // Or find a way to do this before it becomes concurrent.
                            this._maxOffsetPickedUpForHandlingByPartition.AddOrUpdate(message.Offset.Partition,
                                addValueFactory: _ => new OffsetStatus(message.Offset),
                                updateValueFactory: (partition, existing) =>
                                    existing.Offset.Offset < message.Offset.Offset ?
                                        new OffsetStatus(message.Offset) : existing);

                            await this._routingTarget.SendAsync(message, stopToken);
                        }
                    }
                    catch (Exception e)
                    {
                        this._logger.LogError(e, "Error in Kafka poller thread");
                        await Task.Delay(333);
                    }
                }

                this._routingTarget.Complete();
                await this._routingTarget.Completion;

                this._handlingTarget.Complete();
                await this._handlingTarget.Completion;

                this._committerTarget.Complete();
                await this._committerTarget.Completion;
            }
            catch (Exception e)
            {
                this._logger.LogCritical(e, "Fatal error in Kafka poller thread");
            }
        }

        private class OffsetStatus
        {
            public IRecordOffset Offset { get; set; }

            public bool IsCommitted { get; set; }

            public OffsetStatus(IRecordOffset offset, bool isCommitted = false)
            {
                this.Offset = offset;
                this.IsCommitted = isCommitted;
            }
        }
    }
}