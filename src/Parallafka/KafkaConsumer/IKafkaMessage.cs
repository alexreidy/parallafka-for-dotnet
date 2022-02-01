namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// A kafka message with a specific Key and Value.
    /// <seealso cref="KafkaMessage"/>
    /// </summary>
    /// <typeparam name="TKey">The key type</typeparam>
    /// <typeparam name="TValue">The value type</typeparam>
    public interface IKafkaMessage<out TKey, out TValue>
    {
        /// <summary>
        /// The message key
        /// </summary>
        TKey Key { get; }

        /// <summary>
        /// The message value (may be null)
        /// </summary>
        TValue Value { get; }

        /// <summary>
        /// The kafka offset for the message
        /// </summary>
        IRecordOffset Offset { get; }
    }
}