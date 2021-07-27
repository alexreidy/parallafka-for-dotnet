using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka.Tests
{
    public class KafkaConsumerSpy<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        public ConcurrentQueue<IRecordOffset> CommittedOffsets { get; } = new();

        private IKafkaConsumer<TKey, TValue> _backingConsumer;

        public KafkaConsumerSpy(IKafkaConsumer<TKey, TValue> backingConsumer)
        {
            this._backingConsumer = backingConsumer;
        }

        public Task CommitAsync(IEnumerable<IRecordOffset> offsets)
        {
            foreach (var offset in offsets)
            {
                this.CommittedOffsets.Enqueue(offset);
            }
            return this._backingConsumer.CommitAsync(offsets);
        }

        public ValueTask DisposeAsync()
        {
            return this._backingConsumer.DisposeAsync();
        }

        public Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken)
        {
            return this._backingConsumer.PollAsync(cancellationToken);
        }
    }
}