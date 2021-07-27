#pragma warning disable CS4014

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests.Contracts
{
    public abstract class CommitTestsBase : KafkaTopicTestBase
    {
        [Fact]
        public virtual async Task ConsumerHangsAtPartitionEndsTillNewMessageAsync()
        {
            await using(KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync("parallafka"))
            await using(var parallafka = new Parallafka<string, string>(consumer,
                new ParallafkaConfig()
                {
                    MaxConcurrentHandlers = 7,
                }))
            {
                var consumed = new ConcurrentQueue<IKafkaMessage<string, string>>();
                await parallafka.ConsumeAsync(async msg =>
                {
                    consumed.Enqueue(msg);
                });
            }
        }
    }
}