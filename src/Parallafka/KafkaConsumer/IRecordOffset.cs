using System;

namespace Parallafka.KafkaConsumer
{
    /// <summary>
    /// The description of the message partition and offset
    /// </summary>
    public interface IRecordOffset : IEquatable<IRecordOffset>
    {
        /// <summary>
        /// The partition for the record
        /// </summary>
        int Partition { get; }

        /// <summary>
        /// The offset of the record
        /// </summary>
        long Offset { get; }
    }
}