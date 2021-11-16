using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Parallafka.KafkaConsumer;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests
{
    public class ParallafkaBasicTests
    {
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
            var stop = new CancellationTokenSource();
            var offsets = new int[partitions];
            var messages = Enumerable.Range(1, countTotalMessages)
                .Select(i => new KafkaMessage<string, string>(
                    $"key{i % partitions}",
                    $"{i}",
                    new RecordOffset(i % partitions, Interlocked.Increment(ref offsets[i % partitions]))))
                .ToList();

            var consumer = new TestConsumer<string, string>(messages);
            // Parallafka<string, string>.WriteLine = s => this._console.WriteLine(s);

            var pk = new Parallafka<string, string>(consumer, new ParallafkaConfig<string, string>
            {
                Logger = new TestLogger(),
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            });

            var messagesConsumed = 0;

            if (countTotalMessages == 0)
            {
                stop.Cancel();
            }

            await pk.ConsumeAsync(async m =>
                {
                    await Task.Yield();

                    if (Interlocked.Increment(ref messagesConsumed) == countTotalMessages)
                    {
                        stop.Cancel();
                    }
                },
                stop.Token);

            Assert.Equal(countTotalMessages, messagesConsumed);

            if (countTotalMessages > 0)
            {
                var lastCommit = consumer.Commits.Last();
                Assert.Equal(offsets[lastCommit.Partition % partitions], lastCommit.Offset);
            }
            else
            {
                Assert.Empty(consumer.Commits);
            }
        }

        private class TestLogger : ILogger
        {
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                return new D();
            }

            private class D : IDisposable
            {
                public void Dispose()
                {
                }
            }
        }

        private class TestConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
        {
            private readonly IEnumerator<IKafkaMessage<TKey, TValue>> _enumerator;

            public TestConsumer(IEnumerable<IKafkaMessage<TKey, TValue>> messages)
            {
                this._enumerator = messages.GetEnumerator();
            }

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

                return _enumerator.Current;
            }

            public Task CommitAsync(IRecordOffset offset)
            {
                lock (this.Commits)
                {
                    this.Commits.Add(offset);
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
