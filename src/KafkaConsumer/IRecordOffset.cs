namespace Parallafka.KafkaConsumer
{
    public interface IRecordOffset
    {
        int Partition { get; }

        long Offset { get; }
    }
}