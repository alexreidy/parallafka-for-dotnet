using System;

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

        public override bool Equals(object obj)
        {
            return this.Equals(obj as IRecordOffset);
        }

        public bool Equals(IRecordOffset other)
        {
            if (object.ReferenceEquals(this, null))
            {
                return false;
            }
            if (object.ReferenceEquals(this, other))
            {
                return true;
            }
            return other.Offset == this.Offset && other.Partition == this.Partition;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.Partition, this.Offset);
        }
    }
}