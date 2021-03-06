using System;
using System.Threading.Tasks;
using Parallafka.IntegrationTests;
using Parallafka.Tests;
using Parallafka.Tests.Contracts;
using Xunit;
using Xunit.Abstractions;

namespace ConfluentKafkaAdapter.Tests
{
    public class ConsumerPollTests : ConsumerPollTestsBase
    {
        private readonly RealKafkaTopicProvider _topic = new($"ParallafkaConsumerPollTests-{Guid.NewGuid()}");
        
        protected override ITestKafkaTopic Topic => this._topic;

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
        }
    }
}