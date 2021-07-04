namespace Parallafka.KafkaConsumer
{
    public class RecordOffset : IRecordOffset
    {
        public RecordOffset(int partition, long offset)
        {
            this.Partition = partition;
            this.Offset = offset;
        }
        
        public int Partition { get; }

        public long Offset { get; }
    }
}