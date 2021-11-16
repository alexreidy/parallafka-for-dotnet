using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
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

        public CommitState()
        {
            this._messagesNotYetCommittedByPartition = new();
            this._messagesNotYetCommittedByPartitionReaderWriterLock = new();
        }

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
                IKafkaMessage<TKey, TValue> messageToCommit = null;

                lock (queue)
                {
                    while (queue.TryPeek(out var msg) && msg.WasHandled)
                    {
                        if (messageToCommit == null || msg.Offset.Offset > messageToCommit.Offset.Offset)
                        {
                            messageToCommit = msg;
                        }

                        queue.Dequeue();
                    }
                }

                if (messageToCommit != null)
                {
                    yield return messageToCommit;
                }
            }
        }

        public bool TryGetMessageToCommit(IKafkaMessage<TKey, TValue> message, out IKafkaMessage<TKey, TValue> messageToCommit)
        {
            messageToCommit = null;
            Queue<IKafkaMessage<TKey, TValue>> messagesNotYetCommitted;

            this._messagesNotYetCommittedByPartitionReaderWriterLock.EnterReadLock();
            try
            {
                if (!this._messagesNotYetCommittedByPartition.TryGetValue(message.Offset.Partition, out messagesNotYetCommitted))
                {
                    Parallafka<TKey, TValue>.WriteLine($"CS:GetMsgToCommit: {message.Key} {message.Offset} [none]");
                    return false;
                }
            }
            finally
            {
                this._messagesNotYetCommittedByPartitionReaderWriterLock.ExitReadLock();
            }

            lock (messagesNotYetCommitted)
            {
                while (messagesNotYetCommitted.TryPeek(out IKafkaMessage<TKey, TValue> msg) && 
                       msg.WasHandled && 
                       msg.Offset.Offset <= message.Offset.Offset)
                {
                    if (messageToCommit == null || msg.Offset.Offset > messageToCommit.Offset.Offset)
                    {
                        messageToCommit = msg;
                    }

                    messagesNotYetCommitted.Dequeue();

                    Parallafka<TKey, TValue>.WriteLine($"CS:DequeueMessage: {msg.Key} {msg.Offset}");
                }

                Parallafka<TKey, TValue>.WriteLine($"CS:GetMsgToCommit: {message.Key} {message.Offset} {(messageToCommit == null ? "[notfound]" : messageToCommit.Offset )}");

                return messageToCommit != null;
            }
        }

        public void EnqueueMessage(IKafkaMessage<TKey, TValue> message)
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
