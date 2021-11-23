using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests
{
    public abstract class KafkaTopicTestBase : IAsyncLifetime
    {
        protected ITestOutputHelper Console { get; }

        protected abstract ITestKafkaTopic Topic { get; }

        protected KafkaTopicTestBase(ITestOutputHelper console)
        {
            this.Console = console;
        }

        public virtual Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public virtual Task DisposeAsync()
        {
            return this.Topic.DeleteAsync();
        }

        protected IEnumerable<IKafkaMessage<string, string>> GenerateTestMessages(int count, int startNum = 1, bool duplicateKeys = true)
        {
            return Enumerable.Range(startNum, count).Select(i => new KafkaMessage<string, string>(
                    key: $"k{(duplicateKeys && i % 9 == 0 ? i - 1 : i)}",
                    value: $"Message {i}",
                    offset: null));
        }

        protected async Task<List<IKafkaMessage<string, string>>> PublishTestMessagesAsync(int count, int startNum = 1, bool duplicateKeys = true)
        {
            var messages = this.GenerateTestMessages(count, startNum, duplicateKeys).ToList();
            await this.Topic.PublishAsync(messages);
            return messages;
        }

        protected async Task PublishTestMessagesUntilCancelAsync(CancellationToken stopToken)
        {
            await Task.Yield();
            int batchSize = 500;
            for (int i = 0; !stopToken.IsCancellationRequested; i += batchSize)
            {
                await this.PublishTestMessagesAsync(batchSize, startNum: 1 + i);
            }
        }

    }
}