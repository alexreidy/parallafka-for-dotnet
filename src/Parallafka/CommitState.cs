using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    /// <summary>
    /// Keeps track of messages that need to be committed.a
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
        private readonly Dictionary<int, Queue<IKafkaMessage<TKey, TValue>>> _messagesNotYetCommittedByPartition;
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
            List<Queue<IKafkaMessage<TKey, TValue>>> allQueues;

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
                IKafkaMessage<TKey, TValue> messageToCommit = GetMessageToCommit(null, queue);

                if (messageToCommit != null)
                {
                    yield return messageToCommit;
                }
            }
        }

        /// <summary>
        /// Given a handled message, find a message to commit up to and including the message
        /// </summary>
        /// <param name="message">The message that could be committed</param>
        /// <param name="messageToCommit">The message that should be committed</param>
        /// <returns>True if a message was found, false otherwise</returns>
        public bool TryGetMessageToCommit(IKafkaMessage<TKey, TValue> message, [NotNullWhen(true)] out IKafkaMessage<TKey, TValue> messageToCommit)
        {
            messageToCommit = null;
            Queue<IKafkaMessage<TKey, TValue>> messagesInPartition;

            this._messagesNotYetCommittedByPartitionReaderWriterLock.EnterReadLock();
            try
            {
                if (!this._messagesNotYetCommittedByPartition.TryGetValue(message.Offset.Partition, out messagesInPartition))
                {
                    Parallafka<TKey, TValue>.WriteLine($"CS:GetMsgToCommit: {message.Key} {message.Offset} [none]");
                    return false;
                }
            }
            finally
            {
                this._messagesNotYetCommittedByPartitionReaderWriterLock.ExitReadLock();
            }

            messageToCommit = GetMessageToCommit(message,messagesInPartition);
            return messageToCommit != null;
        }

        private IKafkaMessage<TKey, TValue> GetMessageToCommit(IKafkaMessage<TKey, TValue> message, Queue<IKafkaMessage<TKey, TValue>> messagesInPartition)
        {
            IKafkaMessage<TKey, TValue> messageToCommit = null;
            lock (messagesInPartition)
            {
                while (messagesInPartition.TryPeek(out IKafkaMessage<TKey, TValue> msg) &&
                       msg.WasHandled &&
                       (message == null || msg.Offset.Offset <= message.Offset.Offset))
                {
                    messageToCommit = msg;
                    messagesInPartition.Dequeue();

                    this._canQueueMessage.Release();

                    Parallafka<TKey, TValue>.WriteLine($"CS:DequeueMessage: {msg.Key} {msg.Offset}");
                }

                if (message == null)
                {
                    Parallafka<TKey, TValue>.WriteLine(
                        $"CS:GetMsgToCommit: [any] {(messageToCommit == null ? "[notfound]" : messageToCommit.Offset)}");
                }
                else
                {
                    Parallafka<TKey, TValue>.WriteLine(
                        $"CS:GetMsgToCommit: {message.Key} {message.Offset} {(messageToCommit == null ? "[notfound]" : messageToCommit.Offset)}");
                }

                return messageToCommit;
            }
        }

        /// <summary>
        /// Adds the message to the commit queue to be committed after it is handled
        /// </summary>
        /// <param name="message"></param>
        public async Task EnqueueMessageAsync(IKafkaMessage<TKey, TValue> message)
        {
            Queue<IKafkaMessage<TKey, TValue>> messagesInPartition;

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

            lock (messagesInPartition)
            {
                messagesInPartition.Enqueue(message);
                Parallafka<TKey, TValue>.WriteLine($"CS:EnqueueMessage: {message.Key} {message.Offset}");
            }
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
