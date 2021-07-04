namespace Parallafka.KafkaConsumer
{
    public class KafkaMessage<TKey, TValue> : IKafkaMessage<TKey, TValue>
    {
        public KafkaMessage(TKey key, TValue value, IRecordOffset offset)
        {
            this.Key = key;
            this.Value = value;
            this.Offset = offset;
        }

        public TKey Key { get; set; }

        public TValue Value { get; set; }

        public IRecordOffset Offset { get; set; }

        public bool WasHandled { get; set; }
    }
}