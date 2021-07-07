using System.Threading.Tasks;
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
    }
}