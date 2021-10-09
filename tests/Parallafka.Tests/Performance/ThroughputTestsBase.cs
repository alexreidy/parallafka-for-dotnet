using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.OrderGuarantee;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Performance
{
    public abstract class ThroughputTestsBase : KafkaTopicTestBase
    {
        protected virtual int RecordCount { get; } = 500;

        protected virtual int StartTimingAfterConsumingWarmupMessages { get; } = 25;

        private IEnumerable<IKafkaMessage<string, string>> _sentMessages { get; set; }

        private ITestOutputHelper _output;

        public ThroughputTestsBase(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public virtual async Task TestCanConsumeMuchFasterThanDefaultConsumerAsync()
        {
            var sw = Stopwatch.StartNew();
            this._sentMessages = await this.PublishTestMessagesAsync(this.RecordCount);
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
            await using(KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync("rawConsumer"))
            {
                TimeSpan duration = await this.TimeConsumerAsync(consumer, consumeAllAsync: async (Func<IKafkaMessage<string, string>, Task> consumeAsync) =>
                {
                    IKafkaMessage<string, string> message;
                    while ((message = await consumer.PollAsync(new CancellationTokenSource(5000).Token)) != null)
                    {
                        await consumeAsync(message);
                        await consumer.CommitAsync(new[] { message.Offset });
                    }
                });
                return duration;
            }
        }

        private async Task<TimeSpan> TimeParallafkaConsumerAsync()
        {
            await using(KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync("parallafka"))
            {
                await using(IParallafka<string, string> parallafka = new Parallafka<string, string>(
                    consumer,
                    new ParallafkaConfig<string, string>()
                    {
                        MaxConcurrentHandlers = 7,
                    }))
                {
                    TimeSpan duration = await this.TimeConsumerAsync(
                        consumer: consumer,
                        consumeAllAsync: parallafka.ConsumeAsync,
                        onFinishedAsync: () => Task.Run(parallafka.DisposeAsync));
                    return duration;
                }
            }
        }

        private async Task<TimeSpan> TimeConsumerAsync(
            KafkaConsumerSpy<string, string> consumer,
            Func<Func<IKafkaMessage<string, string>, Task>, Task> consumeAllAsync,
            Func<Task> onFinishedAsync = null)
        {
            var consumptionVerifier = new ConsumptionVerifier();
            consumptionVerifier.AddSentMessages(this._sentMessages);

            int totalHandled = 0;
            bool isWarmup = true;
            Stopwatch sw = new();
            var rngs = new ThreadSafeRandom();
            await consumeAllAsync(async message =>
            {
                consumptionVerifier.AddConsumedMessages(new[] { message });
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
                    
                    if (Interlocked.Increment(ref totalHandled) == this.RecordCount)
                    {
                        if (onFinishedAsync != null)
                        {
                            await onFinishedAsync();
                        }
                    }
                }
            });

            sw.Stop();

            if (totalHandled < this.RecordCount)
            {
                throw new Exception($"Only consumed {totalHandled} of {this.RecordCount}. Is PollAsync returning null?");
            }

            consumptionVerifier.AssertConsumedAllSentMessagesProperly();
            consumptionVerifier.AssertAllConsumedMessagesWereCommitted(consumer);

            return sw.Elapsed;
        }
    }
}