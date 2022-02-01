namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// The message from kafka - a key, value and offset
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    internal class KafkaMessageWrapped<TKey, TValue>
    {
        /// <summary>
        /// Creates a new instance of a <see cref="KafkaMessageWrapped{TKey, TValue}"/> for a specific message
        /// </summary>
        public KafkaMessageWrapped(IKafkaMessage<TKey, TValue> message)
        {
            this.Message = message;
        }

        public IKafkaMessage<TKey, TValue> Message { get; }

        /// <summary>
        /// The Key for the message
        /// </summary>
        public TKey Key => this.Message.Key;

        /// <summary>
        /// The Value for the message (may be null)
        /// </summary>
        public TValue Value => this.Message.Value;

        /// <summary>
        /// The record off for the message
        /// </summary>
        public IRecordOffset Offset => this.Message.Offset;

        public bool ReadyToCommit { get; private set; }

        public void SetIsReadyToCommit()
        {
            this.ReadyToCommit = true;
        }
    }

    internal static class KafkaMessageWrappedExtensions
    {
        public static KafkaMessageWrapped<TKey, TValue> Wrapped<TKey, TValue>(this IKafkaMessage<TKey, TValue> message)
        {
            return new KafkaMessageWrapped<TKey, TValue>(message);
        }
    }
}