using System;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    /// <summary>
    /// Describes the configuration for <see cref="Parallafka{TKey,TValue}"/>
    /// </summary>
    public interface IParallafkaConfig
    {
        /// <summary>
        /// The maximum degree of parallelism for handling messages.  More parallelism = more throughput, but more resources
        /// </summary>
        int MaxDegreeOfParallelism { get; }

        /// <summary>
        /// Gets the logger to use
        /// </summary>
        ILogger Logger { get; }

        /// <summary>
        /// The maximum delay for committing messages to Kafka.  Use TimeSpan.Zero to commit as quickly as possible after processing the message.
        /// </summary>
        TimeSpan CommitDelay { get; }

        /// <summary>
        /// Gets the maximum queued messages.  Defaults to 1000
        /// </summary>
        int MaxQueuedMessages { get; }

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