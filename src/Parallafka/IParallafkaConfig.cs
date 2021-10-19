using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public interface IParallafkaConfig<TKey, TValue>
    {
        int MaxConcurrentHandlers { get; }

        Func<Parallafka<TKey, TValue>, IDisposeStrategy> DisposeStrategyProvider { get; }

        ILogger Logger { get; }

        // TODO: adaptive mode flag: optimize throughput by tuning thread count

        // /// <summary>
        // /// A single message can potentially keep a handler thread busy for an
        // /// extended period after other "nearby" records have been handled, whether it's retrying or
        // /// otherwise slow to finish processing. This setting puts a limit on how far ahead available handlers
        // /// can continue consuming the topic while the slow outlier holds up more recent "ready" commits.
        // /// </summary>
        // long? PauseConsumptionWhenUncommittedRecordCountExceeds { get; }

        // /// <summary>
        // /// Called when the uncommitted record count exceeds the setting
        // /// `PauseConsumptionWhenUncommittedRecordCountExceeds`.
        // /// </summary>
        // Func<Task> OnUncommittedRecordCountExceedsThresholdAsync { get; }
    }
}