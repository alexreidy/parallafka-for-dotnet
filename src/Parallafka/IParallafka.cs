using System;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    public interface IParallafka<TKey, TValue> : IAsyncDisposable
    {
        Task ConsumeAsync(Func<IKafkaMessage<TKey, TValue>, Task> consumeAsync);
    }
}