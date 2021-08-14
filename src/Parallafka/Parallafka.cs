using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

#pragma warning disable CS4014

namespace Parallafka
{
    public class Parallafka<TKey, TValue> : IParallafka<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;

        private readonly IParallafkaConfig<TKey, TValue> _config;

        private readonly CancellationTokenSource _pollerShutdownCts = new();

        private bool _pollerIsRunning = false;

        private readonly CancellationTokenSource _handlerShutdownCts = new();

        private int _nHandlerThreadsRunning = 0;

        private bool _handlerThreadsAreRunning => this._nHandlerThreadsRunning > 0;

        private readonly CancellationTokenSource _controllerShutdownCts = new();

        private bool _controllerIsRunning = false;

        /// <summary>
        /// A bounded buffer for messages polled from Kafka.
        /// </summary>
        private readonly BlockingCollection<IKafkaMessage<TKey, TValue>> _polledMessageQueue;

        /// <summary>
        /// Messages eligible for handler threads to pick up and handle.
        /// </summary>
        private readonly BlockingCollection<IKafkaMessage<TKey, TValue>> _messagesReadyForHandling;

        /// <summary>
        /// This is where messages go after being handled: enqueued to be committed when it's safe.
        /// </summary>
        private readonly BlockingCollection<IKafkaMessage<TKey, TValue>> _handledMessagesNotYetCommitted;

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

        public Parallafka(IKafkaConsumer<TKey, TValue> consumer, IParallafkaConfig<TKey, TValue> config)
        {
            this._consumer = consumer;
            this._config = config;

            // TODO: Configurable caps, good defaults
            this._polledMessageQueue = new BlockingCollection<IKafkaMessage<TKey, TValue>>(15000);
            this._messagesReadyForHandling = new BlockingCollection<IKafkaMessage<TKey, TValue>>(15000);
            this._handledMessagesNotYetCommitted = new BlockingCollection<IKafkaMessage<TKey, TValue>>();
            this._messagesToHandleForKey = new Dictionary<TKey, Queue<IKafkaMessage<TKey, TValue>>>();
            this._messagesNotYetCommittedByPartition = new Dictionary<int, Queue<IKafkaMessage<TKey, TValue>>>();
        }

        public async Task ConsumeAsync(Func<IKafkaMessage<TKey, TValue>, Task> consumeAsync)
        {
            this.StartKafkaPollerThread();
            this.StartHandlerThreads(consumeAsync);

            this._controllerIsRunning = true;
            await Task.Yield();
            while (!this._controllerShutdownCts.IsCancellationRequested) // TODO
            {
                // TODO: Probably want to give CPU a break here
                bool gotOne = this._polledMessageQueue.TryTake(out IKafkaMessage<TKey, TValue> message, millisecondsTimeout: 0);
                if (gotOne)
                {
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

                        this._messagesReadyForHandling.Add(message);
                    }

                    if (!this._messagesNotYetCommittedByPartition.TryGetValue(message.Offset.Partition,
                        out Queue<IKafkaMessage<TKey, TValue>> messagesInPartition))
                    {
                        messagesInPartition = new Queue<IKafkaMessage<TKey, TValue>>();
                        this._messagesNotYetCommittedByPartition[message.Offset.Partition] = messagesInPartition;
                    }
                    messagesInPartition.Enqueue(message);
                }

                if (this._handledMessagesNotYetCommitted.TryTake(out IKafkaMessage<TKey, TValue> handledMessage))
                {
                    Queue<IKafkaMessage<TKey, TValue>> messagesNotYetCommitted = this._messagesNotYetCommittedByPartition[handledMessage.Offset.Partition];

                    if (messagesNotYetCommitted.Count > 0 && handledMessage == messagesNotYetCommitted.Peek())
                    {
                        var messagesToCommit = new List<IKafkaMessage<TKey, TValue>>();
                        while (messagesNotYetCommitted.TryPeek(out IKafkaMessage<TKey, TValue> msg) && msg.WasHandled) // RCs here?
                        {
                            messagesToCommit.Add(msg);
                            messagesNotYetCommitted.Dequeue();
                        }

                        while (!this._controllerShutdownCts.IsCancellationRequested)
                        {
                            try
                            {
                                // TODO: inject CancelToken for hard-stop strategy?
                                await this._consumer.CommitAsync(messagesToCommit.Select(m => m.Offset));
                                break;
                            }
                            catch (Exception e)
                            {
                                // TODO: log, delay
                                Console.WriteLine(e);
                            }
                        }
                    }

                    // Is this safe as far as commits?
                    // If there are any messages with the same key queued, make the next one available for handling.
                    if (this._messagesToHandleForKey.TryGetValue(handledMessage.Key, out Queue<IKafkaMessage<TKey, TValue>> messagesQueuedForKey))
                    {
                        if (messagesQueuedForKey == null || messagesQueuedForKey.Count == 0)
                        {
                            this._messagesToHandleForKey.Remove(handledMessage.Key);
                        }
                        else
                        {
                            this._messagesReadyForHandling.Add(messagesQueuedForKey.Dequeue());
                        }
                    }
                }
            }

            this._controllerIsRunning = false;
        }

        private void StartHandlerThreads(Func<IKafkaMessage<TKey, TValue>, Task> consumeAsync)
        {
            for (int i = 0; i < this._config.MaxConcurrentHandlers; i++)
            {
                Task.Run(async () =>
                {
                    Interlocked.Increment(ref this._nHandlerThreadsRunning);

                    while (!this._handlerShutdownCts.IsCancellationRequested)
                    {
                        IKafkaMessage<TKey, TValue> message = null;
                        try
                        {
                            message = this._messagesReadyForHandling.Take(this._handlerShutdownCts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }

                        try
                        {
                            await consumeAsync(message);
                        }
                        catch (Exception e)
                        {
                            // TODO: Injected logger
                            Console.WriteLine(e);
                            // TODO: User is responsible for handling errors but should we do anything else here?
                        }
                        
                        message.WasHandled = true;
                        this._handledMessagesNotYetCommitted.Add(message);
                    }

                    Interlocked.Decrement(ref this._nHandlerThreadsRunning);
                });
            }
        }

        private void StartKafkaPollerThread()
        {
            Task.Run(async () =>
            {
                this._pollerIsRunning = true;
                try
                {
                    while (!this._pollerShutdownCts.IsCancellationRequested)
                    {
                        // TODO: Error handling
                        IKafkaMessage<TKey, TValue> message = await this._consumer.PollAsync(this._pollerShutdownCts.Token);
                        if (message != null)
                        {
                            this._polledMessageQueue.Add(message);
                        }
                        else
                        {
                            // TODO: Log error if not cancelled. This is a breach of contract.
                            await Task.Delay(50);
                        }
                    }
                }
                catch (Exception e)
                {
                    // TODO: log
                }
                finally
                {
                    this._pollerIsRunning = false;
                }
            });
        }

        public ValueTask DisposeAsync()
        {
            return this._config.DisposeStrategyProvider.Invoke(this).DisposeAsync();
            //return new HardStopDisposeStrategy(this).DisposeAsync();
            //return new GracefulShutdownDisposeStrategy(this, TimeSpan.FromSeconds(15)).DisposeAsync();
        }

        public class HardStopDisposeStrategy : IDisposeStrategy
        {
            private Parallafka<TKey, TValue> _parallafka;

            public HardStopDisposeStrategy(Parallafka<TKey, TValue> parallafka)
            {
                this._parallafka = parallafka;
            }

            public async ValueTask DisposeAsync()
            {
                this._parallafka._handlerShutdownCts.Cancel();
                this._parallafka._pollerShutdownCts.Cancel();
                this._parallafka._controllerShutdownCts.Cancel();

                while (this._parallafka._controllerIsRunning || this._parallafka._pollerIsRunning)
                {
                    await Task.Delay(10);
                }

                //await this._parallafka._consumer.DisposeAsync();todo
            }
        }

        public class GracefulShutdownDisposeStrategy : IDisposeStrategy
        {
            private Parallafka<TKey, TValue> _parallafka;
            private TimeSpan? _waitTimeout;

            public GracefulShutdownDisposeStrategy(
                Parallafka<TKey, TValue> parallafka,
                TimeSpan? waitTimeout = null)
            {
                this._parallafka = parallafka;
                this._waitTimeout = waitTimeout;
            }

            public async ValueTask DisposeAsync()
            {
                this._parallafka._handlerShutdownCts.Cancel();
                this._parallafka._pollerShutdownCts.Cancel();

                var timeoutTask = Task.Delay(this._waitTimeout ?? TimeSpan.FromMilliseconds(int.MaxValue)); // todo
                try
                {
                    await this.WaitForPollerToStopAsync(timeoutTask);
                    await this.WaitForHandlersToStopAsync(timeoutTask);
                    await this.WaitForControllerToStopAsync(timeoutTask);
                }
                catch (Exception)
                {
                    // TODO: log
                    throw;
                }
                finally
                {
                    this._parallafka._controllerShutdownCts.Cancel();
                    //await this._parallafka._consumer.DisposeAsync();todo
                }
            }

            private async Task WaitForPollerToStopAsync(Task timeoutTask)
            {
                while (this._parallafka._pollerIsRunning)
                {
                    await Task.Delay(10);
                    if (timeoutTask.IsCompleted)
                    {
                        throw new Exception("Timed out waiting for poller to shut down");
                    }
                }
            }

            private async Task WaitForHandlersToStopAsync(Task timeoutTask)
            {
                while (this._parallafka._handlerThreadsAreRunning)
                {
                    await Task.Delay(10);
                    if (timeoutTask.IsCompleted)
                    {
                        throw new Exception($"Timed out waiting for {this._parallafka._nHandlerThreadsRunning} handlers to finish and shut down");
                    }
                }
            }

            private async Task WaitForControllerToStopAsync(Task timeoutTask)
            {
                // wtf is this:

                // Assuming handlers and poller have stopped.
                while (this._parallafka._controllerIsRunning)
                {
                    //this._parallafka._messagesNotYetCommittedByPartition // wait for empty pipes (commits) and then stop controller.

                    if (this._parallafka._handledMessagesNotYetCommitted.Count == 0)
                    {
                        bool allZero = true;
                        foreach (var kvp in this._parallafka._messagesNotYetCommittedByPartition)
                        {
                            // TODO: thread safety
                            if (kvp.Value == null)
                            {
                                continue;
                            }
                            if (kvp.Value.Count != 0)
                            {
                                allZero = false;
                                break;
                            }
                        }
                        if (allZero)
                        {
                            this._parallafka._controllerShutdownCts.Cancel();
                            return;
                        }
                    }

                    await Task.Delay(10);
                    if (timeoutTask.IsCompleted)
                    {
                        throw new Exception("Timed out waiting for Parallafka to shut down");
                    }
                }
            }
        }
    }

    public interface IDisposeStrategy
    {
        ValueTask DisposeAsync();
    }
}