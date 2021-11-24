using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// A single consumer instance of a certain Kafka topic.
    /// </summary>
    /// <typeparam name="TKey">The record key type.</typeparam>
    /// <typeparam name="TValue">The record value type.</typeparam>
    public interface IKafkaConsumer<TKey, TValue> : IAsyncDisposable
    {
        /// <summary>
        /// Polls Kafka for the next record.
        /// Returns null iff the cancellationToken is cancelled.
        /// </summary>
        Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken); // TODO: What's the contract as far as nulls, op cancelled ex

        Task CommitAsync(IKafkaMessage<TKey, TValue> message);
    }
}