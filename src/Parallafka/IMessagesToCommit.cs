using System.Collections.Generic;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    internal interface IMessagesToCommit<TKey, TValue>
    {
        /// <summary>
        /// Returns an enumeration of messages that are ready to be committed to kafka.
        /// The commit queues are emptied as much as possible during this enumeration.
        /// </summary>
        IEnumerable<KafkaMessageWrapped<TKey, TValue>> GetMessagesToCommit();
    }
}