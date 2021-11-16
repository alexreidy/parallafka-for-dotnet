using System;

namespace Parallafka.KafkaConsumer
{
    public class KafkaMessage<TKey, TValue> : IKafkaMessage<TKey, TValue>
    {
        public KafkaMessage(TKey key, TValue value, IRecordOffset offset = null)
        {
            this.Key = key;
            this.Value = value;
            this.Offset = offset;
        }

        public TKey Key { get; }

        public TValue Value { get; }

        public IRecordOffset Offset { get; }

        public bool WasHandled { get; set; }
    }
}