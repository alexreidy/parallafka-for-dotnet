using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka.KafkaConsumer.Implementations.Mock
{
    public class MockConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        private readonly Queue<IKafkaMessage<TKey, TValue>> _topic = new Queue<IKafkaMessage<TKey, TValue>>();

        public Dictionary<int, long> OffsetForPartition = new Dictionary<int, long>();

        public MockConsumer()
        {
        }

        public void AddRecords(IEnumerable<IKafkaMessage<TKey, TValue>> messages)
        {
            foreach (var message in messages)
            {
                this._topic.Enqueue(message);
            }
        }

        public Task CommitAsync(IEnumerable<IRecordOffset> offsets)
        {
            foreach (var offset in offsets)
            {
                if (!this.OffsetForPartition.TryGetValue(offset.Partition, out long offsetNum))
                {
                    offsetNum = -1;
                }
                if (offset.Offset > offsetNum)
                {
                    this.OffsetForPartition[offset.Partition] = offset.Offset;
                }
            }
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            throw new System.NotImplementedException();
        }

        public Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(this._topic.Dequeue());
        }
    }
}