using System;

namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// Describes the partition/offset of the message
    /// </summary>
    public class RecordOffset : IRecordOffset
    {
        /// <summary>
        /// Creates an instance of <see cref="RecordOffset"/> with the specified partition and offset
        /// </summary>
        /// <param name="partition">The message partition</param>
        /// <param name="offset">The message offset</param>
        public RecordOffset(int partition, long offset)
        {
            this.Partition = partition;
            this.Offset = offset;
        }
        
        /// <inheritdoc />
        public int Partition { get; }

        /// <inheritdoc />
        public long Offset { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return this.Equals(obj as IRecordOffset);
        }

        /// <inheritdoc />
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

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return HashCode.Combine(this.Partition, this.Offset);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"P:{this.Partition} O:{this.Offset}";
        }
    }
}