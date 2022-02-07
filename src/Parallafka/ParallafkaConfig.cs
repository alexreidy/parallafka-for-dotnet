using System;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public class ParallafkaConfig<TKey, TValue> : IParallafkaConfig
    {
        /// <inheritdoc />
        /// <remarks>Defaults to 3</remarks>
        public int MaxDegreeOfParallelism { get; set; } = 3;

        /// <inheritdoc />
        public ILogger Logger { get; set; } =
            LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<IParallafka<TKey, TValue>>();

        /// <inheritdoc />
        /// <remarks>Defaults to 5 seconds</remarks>
        public TimeSpan CommitDelay { get; set; } = TimeSpan.FromSeconds(5);

        /// <inheritdoc />
        public int MaxQueuedMessages { get; set; } = 1_000;
    }
}