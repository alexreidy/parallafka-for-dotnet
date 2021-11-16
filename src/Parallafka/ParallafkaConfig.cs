using System;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public class ParallafkaConfig<TKey, TValue> : IParallafkaConfig<TKey, TValue>
    {
        public int MaxDegreeOfParallelism { get; set; } = 3;

        public ILogger Logger { get; set; } =
            LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<IParallafka<TKey, TValue>>();

        public TimeSpan? CommitDelay { get; set; } = null;
    }
}