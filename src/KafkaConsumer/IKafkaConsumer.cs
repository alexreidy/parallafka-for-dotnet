using System;
using System.Collections.Generic;
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
        Task<IKafkaMessage<TKey, TValue>> PollAsync();

        Task CommitAsync(IEnumerable<IRecordOffset> offsets);
    }
}