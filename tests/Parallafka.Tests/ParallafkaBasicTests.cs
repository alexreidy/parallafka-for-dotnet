using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Parallafka.KafkaConsumer;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests
{
    public class ParallafkaBasicTests
    {
        [Theory]
        [InlineData(1234567,100,1)]
        [InlineData(1234567,100,5)]
        [InlineData(12,1000,10)]
        public async Task MaxMessageQueuedIsObeyed(int partitions, int maxMessagesQueued, int maxDegreeOfParallelism)
        {
            // given
            var stop = new CancellationTokenSource();
            var giveUp = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var countTotalMessages = maxMessagesQueued * 2;
            var testCases = new TestCases(partitions, countTotalMessages);
            var consumer = new TestConsumer<string, string>(testCases.Messages);
            var logger = new Mock<ILogger>();
            //Parallafka<string, string>.WriteLine = s => this._console.WriteLine(s);

            var pk = new Parallafka<string, string>(consumer, new ParallafkaConfig<string, string>
            {
                Logger = logger.Object,
                MaxDegreeOfParallelism = maxDegreeOfParallelism,
                MaxQueuedMessages = maxMessagesQueued
            });

            var messagesConsumed = 0;

            // when
            var consumerTask = pk.ConsumeAsync(async m =>
                {
                    Interlocked.Increment(ref messagesConsumed);
                    try
                    {
                        await Task.Delay(-1, stop.Token);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                },
                stop.Token);

            while (consumer.MessagesQueued < maxMessagesQueued)
            {
                await Task.Delay(100, giveUp.Token);
            }

            // then
            await Task.Delay(TimeSpan.FromSeconds(1));
            this._console.WriteLine($"Messages queued before stop: {consumer.MessagesQueued}");
            this._console.WriteLine($"Messages handled before stop: {messagesConsumed}");

            stop.Cancel();
            await consumerTask;

            this._console.WriteLine($"Messages queued after stop: {consumer.MessagesQueued}");
            this._console.WriteLine($"Messages handled after stop: {messagesConsumed}");

            Assert.True(messagesConsumed >= maxMessagesQueued && messagesConsumed <= maxMessagesQueued + 2);
        }

        [Theory]
        [InlineData(42, 0, 7)]
        [InlineData(1, 10, 7)]
        [InlineData(12, 12345, 7)]
        [InlineData(99, 14, 7)]
        [InlineData(3, 993, 7)]
        [InlineData(12, 499, 1)]
        [InlineData(1, 1000, 1)]
        public async Task ConsumedMessagesAndCommitsMatch(int partitions, int countTotalMessages, int maxDegreeOfParallelism)
        {
            // given
            var stop = new CancellationTokenSource();
            var testCases = new TestCases(partitions, countTotalMessages);
            var consumer = new TestConsumer<string, string>(testCases.Messages);
            var logger = new Mock<ILogger>();
            // Parallafka<string, string>.WriteLine = s => this._console.WriteLine(s);

            var pk = new Parallafka<string, string>(consumer, new ParallafkaConfig<string, string>
            {
                Logger = logger.Object,
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            });

            var messagesConsumed = 0;

            if (countTotalMessages == 0)
            {
                stop.Cancel();
            }

            // when
            await pk.ConsumeAsync(async m =>
                {
                    await Task.Yield();

                    if (Interlocked.Increment(ref messagesConsumed) == countTotalMessages)
                    {
                        stop.Cancel();
                    }
                },
                stop.Token);

            // then
            Assert.Equal(countTotalMessages, messagesConsumed);

            if (countTotalMessages > 0)
            {
                var lastCommit = consumer.Commits.Last();
                Assert.Equal(testCases.Offsets[lastCommit.Partition % partitions] - 1, lastCommit.Offset);
            }
            else
            {
                Assert.Empty(consumer.Commits);
            }
        }

        private class TestCases
        {
            public long[] Offsets { get; }
            public List<KafkaMessage<string, string>> Messages { get; }

            public TestCases(int numPartitions, int countTotalMessages)
            {
                this.Offsets = new long[numPartitions];
                this.Messages = Enumerable.Range(1, countTotalMessages)
                    .Select(i => new KafkaMessage<string, string>(
                        $"key{i % numPartitions}",
                        $"{i}",
                        new RecordOffset(i % numPartitions,
                            Interlocked.Increment(ref this.Offsets[i % numPartitions]) - 1)))
                    .ToList();
            }
        }

        private class TestConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
        {
            private readonly IEnumerator<IKafkaMessage<TKey, TValue>> _enumerator;

            private int _messagesQueued;

            public TestConsumer(IEnumerable<IKafkaMessage<TKey, TValue>> messages)
            {
                this._enumerator = messages.GetEnumerator();
            }

            public int MessagesQueued => this._messagesQueued;

            public List<IRecordOffset> Commits { get; } = new List<IRecordOffset>();

            public ValueTask DisposeAsync()
            {
                this._enumerator.Dispose();
                return ValueTask.CompletedTask;
            }

            public async Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return null;
                }

                if (!_enumerator.MoveNext())
                {
                    try
                    {
                        await Task.Delay(-1, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        return null;
                    }
                }

                Interlocked.Increment(ref this._messagesQueued);
                return _enumerator.Current;
            }

            public Task CommitAsync(IKafkaMessage<TKey, TValue> message)
            {
                lock (this.Commits)
                {
                    this.Commits.Add(message.Offset);
                }

                return Task.CompletedTask;
            }
        }


        private readonly ITestOutputHelper _console;

        public ParallafkaBasicTests(ITestOutputHelper console)
        {
            this._console = console;
        }
    }
}
