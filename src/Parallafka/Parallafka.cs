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

        private readonly IParallafkaConfig _config;

        private CancellationTokenSource _shutdownCts = new CancellationTokenSource();

        private CancellationToken ShutdownToken => this._shutdownCts.Token;

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

        private bool _pollerThreadIsRunning = false;

        private bool _mainLoopIsRunning = false;

        public Parallafka(IKafkaConsumer<TKey, TValue> consumer, IParallafkaConfig config)
        {
            this._consumer = consumer;
            this._config = config;

            // TODO: Configurable caps, good defaults
            this._polledMessageQueue = new BlockingCollection<IKafkaMessage<TKey, TValue>>(15000);
            this._messagesReadyForHandling = new BlockingCollection<IKafkaMessage<TKey, TValue>>(15000);
            this._handledMessagesNotYetCommitted = new BlockingCollection<IKafkaMessage<TKey, TValue>>();
            this._messagesToHandleForKey = new Dictionary<TKey, Queue<IKafkaMessage<TKey, TValue>>>();
            this._messagesNotYetCommittedByPartition = new Dictionary<int, Queue<IKafkaMessage<TKey, TValue>>>();

            if (this._config.ShutdownToken != null)
            {
                this._config.ShutdownToken.Register(() =>
                {
                    this._shutdownCts.Cancel();
                });
            }
        }

        public async Task ConsumeAsync(Func<IKafkaMessage<TKey, TValue>, Task> consumeAsync)
        {
            this.StartKafkaPollerThread();

            for (int i = 0; i < this._config.MaxConcurrentHandlers; i++)
            {
                Task.Run(async () =>
                {
                    while (!this.ShutdownToken.IsCancellationRequested)
                    {
                        IKafkaMessage<TKey, TValue> message = this._messagesReadyForHandling.Take(this.ShutdownToken);
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
                });
            }

            this._mainLoopIsRunning = true;

            await Task.Yield();
            
            while (!(this.ShutdownToken.IsCancellationRequested && this._handledMessagesNotYetCommitted.Count == 0))
            {
                bool gotOne = this._polledMessageQueue.TryTake(out IKafkaMessage<TKey, TValue> message, millisecondsTimeout: 5);
                if (gotOne)
                {
                    // TODO: Optimize
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
                        // Add the key to indicate that a message with the key is being handled
                        // so we know to queue up any additional messages with the key.
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
                        while (messagesNotYetCommitted.TryPeek(out IKafkaMessage<TKey, TValue> msg) && msg.WasHandled)
                        {
                            messagesToCommit.Add(msg);
                            messagesNotYetCommitted.Dequeue();
                        }

                        // TODO: Retry
                        await this._consumer.CommitAsync(messagesToCommit.Select(m => m.Offset));
                    }

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

            this._mainLoopIsRunning = false;
            Console.WriteLine("out of loop");
        }

        private void StartKafkaPollerThread()
        {
            Task.Run(async () =>
            {
                this._pollerThreadIsRunning = true;
                try
                {
                    while (!this.ShutdownToken.IsCancellationRequested)
                    {
                        // TODO: Error handling
                        IKafkaMessage<TKey, TValue> message = await this._consumer.PollAsync(this.ShutdownToken);
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
                    this._pollerThreadIsRunning = false;
                }
            });
        }

        public async ValueTask DisposeAsync()
        {
            this._shutdownCts.Cancel();
            var timeoutTask = Task.Delay(10000);
            while (this._pollerThreadIsRunning)
            {
                await Task.Delay(10);
                if (timeoutTask.IsCompleted)
                {
                    throw new Exception("Timed out waiting for Parallafka poller thread to stop");
                }
            }

            ValueTask disposeConsumerTask = this._consumer.DisposeAsync();

            Console.WriteLine("poller thread done");

            timeoutTask = Task.Delay(15000); // todo: configurable? No timeout if just finishing up commits TODO TODO TODO
            while (this._mainLoopIsRunning)
            {
                await Task.Delay(10);
                if (timeoutTask.IsCompleted)
                {
                    throw new Exception("Timed out waiting for Parallafka main loop to stop");
                }
            }

            await disposeConsumerTask;
            Console.WriteLine("consumer disposed");

            // TODO: wait for all handling to stop? probably not a great idea
        }
    }
}