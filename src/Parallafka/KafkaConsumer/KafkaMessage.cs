namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// Helper class to create a KafkaMessage
    /// </summary>
    public static class KafkaMessage
    {
        /// <summary>
        /// Creates a new instance of a <see cref="IKafkaMessage{TKey,TValue}"/> for a give key, value and offset
        /// </summary>
        /// <typeparam name="TKey">The key type</typeparam>
        /// <typeparam name="TValue">The value type</typeparam>
        /// <param name="key">The message key</param>
        /// <param name="value">The message value (may be null)</param>
        /// <param name="offset">The message offset</param>
        /// <returns></returns>
        public static IKafkaMessage<TKey, TValue> Create<TKey, TValue>(
            TKey key, 
            TValue value,
            IRecordOffset offset)
        {
            return new KafkaMessageImpl<TKey, TValue>(key, value, offset);
        }

        private class KafkaMessageImpl<TKey, TValue> : IKafkaMessage<TKey, TValue>
        {
            public KafkaMessageImpl(TKey key, TValue value, IRecordOffset offset)
            {
                this.Key = key;
                this.Value = value;
                this.Offset = offset;
            }

            public TKey Key { get; }
            public TValue Value { get; }
            public IRecordOffset Offset { get; }
        }
    }
}
