using System.Collections.Generic;
using System.Threading.Tasks;

namespace Parallafka.KafkaConsumer.Implementations.Mock
{
    public class MockConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        public Task CommitAsync(IEnumerable<IRecordOffset> offsets)
        {
            throw new System.NotImplementedException();
        }

        public ValueTask DisposeAsync()
        {
            throw new System.NotImplementedException();
        }

        public Task<IKafkaMessage<TKey, TValue>> PollAsync()
        {
            throw new System.NotImplementedException();
        }
    }
}