using System;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka
{
    public class ParallafkaConfig<TKey, TValue> : IParallafkaConfig<TKey, TValue>
    {
        public Func<Parallafka<TKey, TValue>, IDisposeStrategy> DisposeStrategyProvider { get; set; } = self =>
            new Parallafka<TKey, TValue>.GracefulShutdownDisposeStrategy(self, waitTimeout: TimeSpan.FromSeconds(90));

        public int MaxConcurrentHandlers { get; set; } = 3;

        public long? PauseConsumptionWhenUncommittedRecordCountExceeds { get; set; }

        public Func<Task> OnUncommittedRecordCountExceedsThresholdAsync { get; set; }
    }
}