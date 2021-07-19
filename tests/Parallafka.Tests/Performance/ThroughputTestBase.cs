using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Performance
{
    public abstract class ThroughputTestBase : KafkaTopicTestBase
    {
        protected virtual int RecordCount { get; } = 500;

        protected virtual int StartTimingAfterConsumingWarmupMessages { get; } = 25;

        private ITestOutputHelper _output;

        public ThroughputTestBase(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public virtual async Task TestCanConsumeMuchFasterThanDefaultConsumerAsync()
        {
            var sw = Stopwatch.StartNew();
            await this.PublishTestMessagesAsync(this.RecordCount);
            this._output.WriteLine($"Took {sw.ElapsedMilliseconds}ms to connect and publish");
            // Running these timers serially to be fair
            TimeSpan rawConsumerElapsed = await this.TimeRawSingleThreadedConsumerAsync();
            string rawConsumerTimeMsg = $"Raw consumer took {rawConsumerElapsed.TotalMilliseconds}ms";
            this._output.WriteLine(rawConsumerTimeMsg);
            TimeSpan parallafkaElapsed = await this.TimeParallafkaConsumerAsync(); // todo: include the shutdown and don't be done until everything is committed!
            string parallafkaTimeMsg = $"Parallafka consumer took {parallafkaElapsed.TotalMilliseconds}ms";
            this._output.WriteLine(parallafkaTimeMsg);
            Assert.True(rawConsumerElapsed / parallafkaElapsed > 5, $"{rawConsumerTimeMsg}; {parallafkaTimeMsg}");
        }

        private async Task<TimeSpan> TimeRawSingleThreadedConsumerAsync()
        {
            await using(IKafkaConsumer<string, string> consumer = await this.Topic.GetConsumerAsync("rawConsumer"))
            {
                TimeSpan duration = await this.TimeConsumerAsync(consumeAllAsync: async (Func<IKafkaMessage<string, string>, Task> consumeAsync) =>
                {
                    IKafkaMessage<string, string> message;
                    while ((message = await consumer.PollAsync(new CancellationTokenSource(5000).Token)) != null)
                    {
                        await consumeAsync(message);
                    }
                });
                return duration;
            }
        }

        private async Task<TimeSpan> TimeParallafkaConsumerAsync()
        {
            await using(IKafkaConsumer<string, string> consumer = await this.Topic.GetConsumerAsync("parallafka"))
            {
                await using(IParallafka<string, string> parallafka = new Parallafka<string, string>(
                    consumer,
                    new ParallafkaConfig()
                    {
                        MaxConcurrentHandlers = 7,
                    }))
                {
                    TimeSpan duration = await this.TimeConsumerAsync(
                        consumeAllAsync: parallafka.ConsumeAsync,
                        onFinishedAsync: () => parallafka.DisposeAsync().AsTask());
                    return duration;
                }
            }
        }

        private async Task<TimeSpan> TimeConsumerAsync(
            Func<Func<IKafkaMessage<string, string>, Task>, Task> consumeAllAsync,
            Func<Task> onFinishedAsync = null)
        {
            int totalHandled = 0;
            bool isWarmup = true;
            Stopwatch sw = new();
            var rngs = new ThreadSafeRandom();
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
                    await rngs.BorrowAsync(async rng =>
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(80 + rng.Next(40)));
                    });
                    
                    if (Interlocked.Increment(ref totalHandled) == this.RecordCount) // todo: use assertion showing each individual message was handled.
                    {
                        sw.Stop();
                        if (onFinishedAsync != null)
                        {
                            await onFinishedAsync();
                        }
                    }
                }
            });

            if (totalHandled < this.RecordCount)
            {
                throw new Exception($"Only consumed {totalHandled} of {this.RecordCount}. Is PollAsync returning null?");
            }
            return sw.Elapsed;
        }
    }
}