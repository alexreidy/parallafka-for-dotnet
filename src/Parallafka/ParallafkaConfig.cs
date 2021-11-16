using System;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public class ParallafkaConfig<TKey, TValue> : IParallafkaConfig<TKey, TValue>
    {
        /// <inheritdoc />
        public int MaxDegreeOfParallelism { get; set; } = 3;

        /// <inheritdoc />
        public ILogger Logger { get; set; } =
            LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<IParallafka<TKey, TValue>>();

        /// <inheritdoc />
        public TimeSpan? CommitDelay { get; set; } = null;

        /// <inheritdoc />
        public int? MaxQueuedMessages { get; set; } = null;
    }
}