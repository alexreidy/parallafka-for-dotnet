using System.Threading.Tasks;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Contracts
{
    public class ConsumerPollTests : ConsumerPollTestsBase
    {
        protected override ITestKafkaTopic Topic { get; }

        [Fact]
        public override Task ConsumerHangsAtPartitionEndsTillNewMessageAsync()
        {
            return base.ConsumerHangsAtPartitionEndsTillNewMessageAsync();
        }

        [Fact]
        public override Task RawConsumerHangsAtPartitionEndsTillNewMessageOrCancellationAsync()
        {
            return base.RawConsumerHangsAtPartitionEndsTillNewMessageOrCancellationAsync();
        }

        public ConsumerPollTests(ITestOutputHelper console) : base(console)
        {
            this.Topic = new FakeTestKafkaTopic();
        }
    }
}
