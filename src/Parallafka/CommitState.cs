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
    internal class CommitState<TKey, TValue>
    {
        /// <summary>
        /// Maps partition number to a queue of the messages received from that partition.
        /// The front of the queue is the earliest uncommitted message. Messages are removed once committed.
        /// It's safe to commit a handled message provided all earlier (lower-offset) messages have also been marked
        /// as handled.
        /// </summary>
        private readonly Dictionary<int, ConcurrentQueue<IKafkaMessage<TKey, TValue>>> _messagesNotYetCommittedByPartition;
        private readonly ReaderWriterLockSlim _messagesNotYetCommittedByPartitionReaderWriterLock;

        private readonly SemaphoreSlim _canQueueMessage;
        private readonly CancellationToken _stopToken;

        public CommitState(int maxMessagesQueued, CancellationToken stopToken)
        {
            this._canQueueMessage = new(maxMessagesQueued);
            this._stopToken = stopToken;
            this._messagesNotYetCommittedByPartition = new();
            this._messagesNotYetCommittedByPartitionReaderWriterLock = new();
        }

        /// <summary>
        /// Returns an enumeration of messages that are ready to be committed to kafka.
        /// The commit queues are emptied as much as possible during this enumeration.
        /// </summary>
        public IEnumerable<IKafkaMessage<TKey, TValue>> GetMessagesToCommit()
        {
            List<ConcurrentQueue<IKafkaMessage<TKey, TValue>>> allQueues;

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
                IKafkaMessage<TKey, TValue> messageToCommit = GetMessageToCommit(queue);

                if (messageToCommit != null)
                {
                    yield return messageToCommit;
                }
            }
        }

        private IKafkaMessage<TKey, TValue> GetMessageToCommit(ConcurrentQueue<IKafkaMessage<TKey, TValue>> messagesInPartition)
        {
            IKafkaMessage<TKey, TValue> messageToCommit = null;
            while (messagesInPartition.TryPeek(out IKafkaMessage<TKey, TValue> msg) && msg.WasHandled)
            {
                messageToCommit = msg;
                messagesInPartition.TryDequeue(out _);

                this._canQueueMessage.Release();

                Parallafka<TKey, TValue>.WriteLine($"CS:DequeueMessage: {msg.Key} {msg.Offset}");
            }

            Parallafka<TKey, TValue>.WriteLine($"CS:GetMsgToCommit: {(messageToCommit == null ? "[notfound]" : messageToCommit.Offset)}");

            return messageToCommit;
        }

        /// <summary>
        /// Adds the message to the commit queue to be committed after it is handled
        /// </summary>
        /// <param name="message"></param>
        public async Task EnqueueMessageAsync(IKafkaMessage<TKey, TValue> message)
        {
            ConcurrentQueue<IKafkaMessage<TKey, TValue>> messagesInPartition;

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
            await this._canQueueMessage.WaitAsync(-1, this._stopToken);

            messagesInPartition.Enqueue(message);
            Parallafka<TKey, TValue>.WriteLine($"CS:EnqueueMessage: {message.Key} {message.Offset}");
        }

        public string GetStats()
        {
            this._messagesNotYetCommittedByPartitionReaderWriterLock.EnterReadLock();
            try
            {
                return $"{string.Join(", ", this._messagesNotYetCommittedByPartition.Select(kvp => $"P:{kvp.Key} Cnt:{kvp.Value.Count}"))}";
            }
            finally
            {
                this._messagesNotYetCommittedByPartitionReaderWriterLock.ExitReadLock();
            }
        }
    }
}
