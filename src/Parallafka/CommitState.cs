using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    /// <summary>
    /// Keeps track of messages that need to be committed.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class CommitState<TKey, TValue> : IMessagesToCommit<TKey, TValue>
    {
        /// <summary>
        /// Maps partition number to a queue of the messages received from that partition.
        /// The front of the queue is the earliest uncommitted message. Messages are removed once committed.
        /// It's safe to commit a handled message provided all earlier (lower-offset) messages have also been marked
        /// as handled.
        /// </summary>
        private readonly Dictionary<int, ConcurrentQueue<KafkaMessageWrapped<TKey, TValue>>> _messagesNotYetCommittedByPartition;
        private readonly ReaderWriterLockSlim _messagesNotYetCommittedByPartitionReaderWriterLock;

        private readonly SemaphoreSlim _canQueueMessage;
        private readonly CancellationToken _stopToken;

        /// <summary>
        /// Creates a new instance of CommitState
        /// </summary>
        /// <param name="maxMessagesQueued">The maximum messages that can be queued</param>
        /// <param name="stopToken">A token that indicates the commit state should stop accepting new queued messages</param>
        public CommitState(int maxMessagesQueued, CancellationToken stopToken)
        {
            this._canQueueMessage = new(maxMessagesQueued);
            this._stopToken = stopToken;
            this._messagesNotYetCommittedByPartition = new();
            this._messagesNotYetCommittedByPartitionReaderWriterLock = new();
        }

        public event EventHandler OnMessageQueueFull;

        /// <summary>
        /// Returns an enumeration of messages that are ready to be committed to kafka.
        /// The commit queues are emptied as much as possible during this enumeration.
        /// </summary>
        public IEnumerable<KafkaMessageWrapped<TKey, TValue>> GetMessagesToCommit()
        {
            List<ConcurrentQueue<KafkaMessageWrapped<TKey, TValue>>> allQueues;

            this._messagesNotYetCommittedByPartitionReaderWriterLock.EnterReadLock();
            try
            {
                allQueues = this._messagesNotYetCommittedByPartition.Values.ToList();
            }
            finally
            {
                this._messagesNotYetCommittedByPartitionReaderWriterLock.ExitReadLock();
            }

            foreach (var queue in allQueues)
            {
                KafkaMessageWrapped<TKey, TValue> messageToCommit = GetMessageToCommit(queue);

                if (messageToCommit != null)
                {
                    yield return messageToCommit;
                }
            }
        }

        private KafkaMessageWrapped<TKey, TValue> GetMessageToCommit(ConcurrentQueue<KafkaMessageWrapped<TKey, TValue>> messagesInPartition)
        {
            KafkaMessageWrapped<TKey, TValue> messageToCommit = null;
            
            var released = 0;
            while (messagesInPartition.TryPeek(out KafkaMessageWrapped<TKey, TValue> msg) && msg.ReadyToCommit)
            {
                released++;
                messageToCommit = msg;
                messagesInPartition.TryDequeue(out _);
            }

            if (released > 0)
            {
                this._canQueueMessage.Release(released);
            }

            Parallafka<TKey, TValue>.WriteLine($"CS:GetMsgToCommit: {(messageToCommit == null ? "[notfound]" : messageToCommit.Offset)}");

            return messageToCommit;
        }

        /// <summary>
        /// Adds the message to the commit queue to be committed after it is handled
        /// </summary>
        /// <param name="message"></param>
        public async Task EnqueueMessageAsync(KafkaMessageWrapped<TKey, TValue> message)
        {
            ConcurrentQueue<KafkaMessageWrapped<TKey, TValue>> messagesInPartition;

            this._messagesNotYetCommittedByPartitionReaderWriterLock.EnterReadLock();
            try
            {
                this._messagesNotYetCommittedByPartition.TryGetValue(message.Offset.Partition, out messagesInPartition);
            }
            finally
            {
                this._messagesNotYetCommittedByPartitionReaderWriterLock.ExitReadLock();
            }

            if (messagesInPartition == null)
            {
                this._messagesNotYetCommittedByPartitionReaderWriterLock.EnterWriteLock();
                try
                {
                    if (!this._messagesNotYetCommittedByPartition.TryGetValue(message.Offset.Partition, out messagesInPartition))
                    {
                        messagesInPartition = new();
                        this._messagesNotYetCommittedByPartition[message.Offset.Partition] = messagesInPartition;
                    }
                }
                finally
                {
                    this._messagesNotYetCommittedByPartitionReaderWriterLock.ExitWriteLock();
                }
            }

            // ReSharper disable once InconsistentlySynchronizedField
            if (!this._canQueueMessage.Wait(0))
            {
                this.OnMessageQueueFull?.Invoke(this, EventArgs.Empty);
                await this._canQueueMessage.WaitAsync(-1, this._stopToken);
            }

            messagesInPartition.Enqueue(message);
            Parallafka<TKey, TValue>.WriteLine($"CS:EnqueueMessage: {message.Key} {message.Offset}");
        }

        public object GetStats()
        {
            this._messagesNotYetCommittedByPartitionReaderWriterLock.EnterReadLock();
            try
            {
                return this._messagesNotYetCommittedByPartition
                    .Select(kvp => new
                    {
                        Partition = kvp.Key,
                        CommitsPending = kvp.Value.Count
                    });
            }
            finally
            {
                this._messagesNotYetCommittedByPartitionReaderWriterLock.ExitReadLock();
            }
        }
    }
}
