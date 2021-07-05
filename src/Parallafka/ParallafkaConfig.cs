using System;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka
{
    public class ParallafkaConfig : IParallafkaConfig
    {
        public CancellationToken ShutdownToken { get; set; }

        public int MaxConcurrentHandlers { get; set; }

        public long? PauseConsumptionWhenUncommittedRecordCountExceeds { get; set; }

        public Func<Task> OnUncommittedRecordCountExceedsThresholdAsync { get; set; }
    }
}