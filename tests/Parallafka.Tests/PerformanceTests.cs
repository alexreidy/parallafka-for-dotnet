using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.KafkaConsumer.Implementations.Mock;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Performance
{
    public class PerformanceTests
    {
        private ITestOutputHelper _outputHelper;

        public PerformanceTests(ITestOutputHelper outputHelper)
        {
            this._outputHelper = outputHelper;
        }

        [Fact]
        public async Task TestPerformanceAsync()
        {
            int partitionCount = 11;
            int recordsPerPartition = 100;
            var messagesInPartition = new Dictionary<int, IEnumerable<IKafkaMessage<string, string>>>();
            for (int partition = 0; partition < partitionCount; partition++)
            {
                int p = partition;
                messagesInPartition[partition] = Enumerable.Range(1, recordsPerPartition)
                    .Select(i => new KafkaMessage<string, string>(
                        $"k{i}_{p}", $"Message {i} in partition {p}",
                        new RecordOffset(p, i)));
            }

            var consumer1 = new MockConsumer<string, string>();
            foreach (var kvp in messagesInPartition)
            {
                consumer1.AddRecords(kvp.Value);
            }
            Task<TimeSpan> timeRawConsumerTask = this.TimeRawSingleThreadedConsumerAsync(consumer1, recordsPerPartition * partitionCount);

            var consumer2 = new MockConsumer<string, string>();
            foreach (var kvp in messagesInPartition)
            {
                consumer2.AddRecords(kvp.Value);
            }
            Task<TimeSpan> timeParallafkaTask = this.TimeParallafkaConsumerAsync(consumer2, recordsPerPartition * partitionCount);

            await Task.WhenAll(timeRawConsumerTask, timeParallafkaTask);

            // Actually sometimes getting about 13x faster on my machine with MaxConcurrentHandlers == 13.
            // Likewise with 7 threads it's typically (and more consistently) about 7x faster.
            Assert.True(timeRawConsumerTask.Result / timeParallafkaTask.Result > 5);
        }

        private async Task<TimeSpan> TimeParallafkaConsumerAsync(IKafkaConsumer<string, string> consumer, int totalRecords)
        {
            var cts = new CancellationTokenSource(30000);
            var parallafka = new Parallafka<string, string>(consumer,
                new Config()
                {
                    ShutdownToken = cts.Token,
                    MaxConcurrentHandlers = 7,
                });

            int totalHandled = 0;
            var sw = Stopwatch.StartNew();
            await parallafka.ConsumeAsync(async msg =>
            {
                await Task.Delay(10);
                if (Interlocked.Increment(ref totalHandled) == totalRecords)
                {
                    sw.Stop();
                    this._outputHelper.WriteLine($"Parallafka handled all in {sw.Elapsed}");
                    cts.Cancel();
                }
            });

            return sw.Elapsed;
        }

        private async Task<TimeSpan> TimeRawSingleThreadedConsumerAsync(IKafkaConsumer<string, string> consumer, int totalRecords)
        {
            var cts = new CancellationTokenSource(30000);

            int totalHandled = 0; // todo: shared timing handler code
            var sw = Stopwatch.StartNew();
            while (!cts.Token.IsCancellationRequested && await consumer.PollAsync(cts.Token) != null)
            {
                await Task.Delay(10);
                if (Interlocked.Increment(ref totalHandled) == totalRecords)
                {
                    sw.Stop();
                    this._outputHelper.WriteLine($"Raw consumer handled all in {sw.Elapsed}");
                    cts.Cancel();
                }
            }
            return sw.Elapsed;
        }

        [Fact]
        public async Task TestAsync()
        {
            var consumer = new MockConsumer<string, string>();
            var items = new[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "ellamenno", "p" };
            var records = new List<IKafkaMessage<string, string>>();
            for (int offset = 0; offset < items.Length; offset++)
            {
                records.Add(new KafkaMessage<string, string>($"k{offset % 3}", items[offset], new RecordOffset(offset % 2, offset)));
            }
            consumer.AddRecords(records);

            var parallafka = new Parallafka<string, string>(consumer,
                new Config()
                {
                    ShutdownToken = new CancellationTokenSource(10000).Token,
                });
            
            await parallafka.ConsumeAsync(async msg =>
            {
                this._outputHelper.WriteLine("Got message " + msg.Value);
            });
        }
    }

    class Config : IParallafkaConfig
    {
        public CancellationTokenSource Cts { get; set; } = new CancellationTokenSource();

        public CancellationToken ShutdownToken { get; set; }

        public int MaxConcurrentHandlers{ get; set; } = 25;

        public long? PauseConsumptionWhenUncommittedRecordCountExceeds { get; set; }

        public Func<Task> OnUncommittedRecordCountExceedsThresholdAsync{ get; set; } = () => Task.CompletedTask;
    }
}