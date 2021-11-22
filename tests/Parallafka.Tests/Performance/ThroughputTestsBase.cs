using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Performance
{
    public abstract class ThroughputTestsBase : KafkaTopicTestBase
    {
        protected virtual int RecordCount => 500;

        protected virtual int StartTimingAfterConsumingWarmupMessages => 25;

        private IEnumerable<IKafkaMessage<string, string>> _sentMessages;

        protected ThroughputTestsBase(ITestOutputHelper output) : base(output)
        {
        }

        public virtual async Task TestCanConsumeMuchFasterThanDefaultConsumerAsync()
        {
            var sw = Stopwatch.StartNew();
            this._sentMessages = await this.PublishTestMessagesAsync(this.RecordCount);
            this.Console.WriteLine($"Took {sw.ElapsedMilliseconds}ms to connect and publish");
            // Running these timers serially to be fair
            TimeSpan rawConsumerElapsed = await this.TimeRawSingleThreadedConsumerAsync();
            string rawConsumerTimeMsg = $"Raw consumer took {rawConsumerElapsed.TotalMilliseconds}ms";
            this.Console.WriteLine(rawConsumerTimeMsg);
            TimeSpan parallafkaElapsed = await this.TimeParallafkaConsumerAsync(); // todo: include the shutdown and don't be done until everything is committed!
            string parallafkaTimeMsg = $"Parallafka consumer took {parallafkaElapsed.TotalMilliseconds}ms";
            this.Console.WriteLine(parallafkaTimeMsg);
            Assert.True(rawConsumerElapsed / parallafkaElapsed > 5, $"{rawConsumerTimeMsg}; {parallafkaTimeMsg}");
        }

        private async Task<TimeSpan> TimeRawSingleThreadedConsumerAsync()
        {
            await using KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync("rawConsumer");
            TimeSpan duration = await this.TimeConsumerAsync(consumer, consumeAllAsync: async (Func<IKafkaMessage<string, string>, Task> consumeAsync, CancellationToken stop) =>
            {
                IKafkaMessage<string, string> message;
                while ((message = await consumer.PollAsync(new CancellationTokenSource(5000).Token)) != null)
                {
                    await consumeAsync(message);
                    await consumer.CommitAsync(message.Offset);
                }
            });
            return duration;
        }

        private async Task<TimeSpan> TimeParallafkaConsumerAsync()
        {
            await using KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync("parallafka");
            IParallafka<string, string> parallafka = new Parallafka<string, string>(
                consumer,
                new ParallafkaConfig<string, string>()
                {
                    MaxDegreeOfParallelism = 7,
                });
            TimeSpan duration = await this.TimeConsumerAsync(
                consumer: consumer,
                consumeAllAsync: parallafka.ConsumeAsync);
            return duration;
        }

        private async Task<TimeSpan> TimeConsumerAsync(
            KafkaConsumerSpy<string, string> consumer,
            Func<Func<IKafkaMessage<string, string>, Task>, CancellationToken, Task> consumeAllAsync,
            Func<Task> onFinishedAsync = null)
        {
            var consumptionVerifier = new ConsumptionVerifier();
            consumptionVerifier.AddSentMessages(this._sentMessages);

            int totalHandled = 0;
            bool isWarmup = true;
            Stopwatch sw = new();
            CancellationTokenSource stopConsuming = new();
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
                    await Task.Delay(TimeSpan.FromMilliseconds(80 + StaticRandom.Use(r => r.Next(40))));
                    
                    if (Interlocked.Increment(ref totalHandled) == this.RecordCount)
                    {
                        if (onFinishedAsync != null)
                        {
                            await onFinishedAsync();
                        }

                        stopConsuming.Cancel();
                    }
                }
            }, stopConsuming.Token);

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