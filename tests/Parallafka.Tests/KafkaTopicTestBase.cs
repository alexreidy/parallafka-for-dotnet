using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests
{
    public abstract class KafkaTopicTestBase : IAsyncLifetime
    {
        protected abstract ITestKafkaTopic Topic { get; }

        public KafkaTopicTestBase()
        {
        }

        public virtual Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public virtual Task DisposeAsync()
        {
            return this.Topic.DeleteAsync();
        }

        protected IEnumerable<IKafkaMessage<string, string>> GenerateTestMessages(int count, int startNum = 1)
        {
            return Enumerable.Range(startNum, count).Select(i => new KafkaMessage<string, string>(
                    key: $"k{(i % 9 == 0 ? i - 1 : i)}",
                    value: $"Message {i}",
                    offset: null));
        }

        protected async Task<IEnumerable<IKafkaMessage<string, string>>> PublishTestMessagesAsync(int count, int startNum = 1)
        {
            var messages = this.GenerateTestMessages(count, startNum);
            await this.Topic.PublishAsync(messages);
            return messages;
        }
    }
}