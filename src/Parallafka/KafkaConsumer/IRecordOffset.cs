using System;

namespace Parallafka.KafkaConsumer
{
    public interface IRecordOffset : IEquatable<IRecordOffset>
    {
        int Partition { get; }

        long Offset { get; }
    }
}