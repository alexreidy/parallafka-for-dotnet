namespace Parallafka.KafkaConsumer
{
    public interface IKafkaMessage<TKey, TValue>
    {
        TKey Key { get; }

        TValue Value { get; }

        IRecordOffset Offset { get; } // TODO: not null

        bool WasHandled { get; set; } // TODO: this is internal
    }
}