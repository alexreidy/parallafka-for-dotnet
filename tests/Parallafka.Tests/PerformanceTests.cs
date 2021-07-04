using System;
using System.Collections.Generic;
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