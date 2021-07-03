using System;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    public class Parallafka<TKey, TValue> : IParallafka<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;

        private readonly IParallafkaConfig _config;

        public Parallafka(IKafkaConsumer<TKey, TValue> consumer, IParallafkaConfig config)
        {
            this._consumer = consumer;
            this._config = config;
        }

        public Task ConsumeAsync(Func<IKafkaMessage<TKey, TValue>, Task> consumeAsync)
        {
            throw new NotImplementedException();
        }

        public ValueTask DisposeAsync()
        {
            throw new NotImplementedException();
        }
    }
}