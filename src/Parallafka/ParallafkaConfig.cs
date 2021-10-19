using System;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public class ParallafkaConfig<TKey, TValue> : IParallafkaConfig<TKey, TValue>
    {
        public Func<Parallafka<TKey, TValue>, IDisposeStrategy> DisposeStrategyProvider { get; set; } = self =>
            new Parallafka<TKey, TValue>.GracefulShutdownDisposeStrategy(self, waitTimeout: TimeSpan.FromSeconds(30));

        public int MaxConcurrentHandlers { get; set; } = 3;

        public ILogger Logger { get; set; } =
            LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<IParallafka<TKey, TValue>>();
    }
}