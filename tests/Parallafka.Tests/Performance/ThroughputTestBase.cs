using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Performance
{
    public abstract class ThroughputTestBase
    {
        public virtual int RecordCount { get; } = 500;

        protected virtual int StartTimingAfterConsumingWarmupMessages { get; } = 25;

        protected abstract IKafkaConsumer<string, string> GetTestTopicConsumer(string groupId);
        
        protected abstract Task PublishToTestTopicAsync(IEnumerable<IKafkaMessage<string, string>> messages);

        private ITestOutputHelper _output;

        public ThroughputTestBase(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public virtual async Task CanConsumeMuchFasterThanDefaultConsumerAsync()
        {
            var sw = Stopwatch.StartNew();
            await this.PublishTestMessagesAsync(this.RecordCount);
            this._output.WriteLine($"Took {sw.ElapsedMilliseconds}ms to connect and publish");
            // Running these timers serially to be fair
            TimeSpan rawConsumerElapsed = await this.TimeRawSingleThreadedConsumerAsync();
            this._output.WriteLine($"Raw consumer took {rawConsumerElapsed.TotalMilliseconds}");
            TimeSpan parallafkaElapsed = await this.TimeParallafkaConsumerAsync();
            this._output.WriteLine($"Parallafka consumer took {parallafkaElapsed.TotalMilliseconds}");
            Assert.True(rawConsumerElapsed / parallafkaElapsed > 5);
        }

        private async Task<TimeSpan> TimeRawSingleThreadedConsumerAsync()
        {
            await using(IKafkaConsumer<string, string> consumer = this.GetTestTopicConsumer("rawConsumer"))
            {
                TimeSpan duration = await this.TimeConsumerAsync(consumeAllAsync: async (Func<IKafkaMessage<string, string>, Task> consumeAsync) =>
                {
                    IKafkaMessage<string, string> message;
                    while ((message = await consumer.PollAsync(new CancellationTokenSource(100).Token)) != null)
                    {
                        await consumeAsync(message);
                    }
                });
                await consumer.DisposeAsync();
                return duration;
            }
        }

        private async Task<TimeSpan> TimeParallafkaConsumerAsync()
        {
            await using(IKafkaConsumer<string, string> consumer = this.GetTestTopicConsumer("parallafka"))
            {
                var cts = new CancellationTokenSource();
                IParallafka<string, string> parallafka = new Parallafka<string, string>(
                    consumer,
                    new ParallafkaConfig()
                    {
                        MaxConcurrentHandlers = 7,
                        ShutdownToken = cts.Token,
                    });

                TimeSpan duration = await this.TimeConsumerAsync(
                    consumeAllAsync: parallafka.ConsumeAsync,
                    onFinishedAsync: async () => cts.Cancel());
                
                return duration;
            }
        }

        private async Task<TimeSpan> TimeConsumerAsync(
            Func<Func<IKafkaMessage<string, string>, Task>, Task> consumeAllAsync,
            Func<Task> onFinishedAsync = null)
        {
            int totalHandled = 0;
            bool isWarmup = true;
            Stopwatch sw = new();
            var rng = new Random();
            await consumeAllAsync(async message =>
            {
                if (isWarmup)
                {
                    if (Interlocked.Increment(ref totalHandled) == this.StartTimingAfterConsumingWarmupMessages)
                    {
                        isWarmup = false;
                        sw.Start();
                    }
                }
                else
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(80 + rng.Next(40)));
                    if (Interlocked.Increment(ref totalHandled) == this.RecordCount)
                    {
                        sw.Stop();
                        if (onFinishedAsync != null)
                        {
                            await onFinishedAsync();
                        }
                    }
                }
            });
            return sw.Elapsed;
        }

        private Task PublishTestMessagesAsync(int count)
        {
            return this.PublishToTestTopicAsync(
                Enumerable.Range(1, count).Select(i => new KafkaMessage<string, string>(
                    key: $"k{(i % 9 == 0 ? i - 1 : i)}",
                    value: $"Message {i}",
                    offset: null)));
        }
    }
}