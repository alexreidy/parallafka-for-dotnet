using System;
using System.Threading.Tasks;
using Parallafka.Tests;
using Parallafka.Tests.Performance;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.IntegrationTests.Performance
{
    public class ThroughputTests : ThroughputTestsBase
    {
        private readonly ITestKafkaTopic _topic;

        protected override ITestKafkaTopic Topic => this._topic;

        public ThroughputTests(ITestOutputHelper output) : base(output)
        {
            this._topic = new TestKafkaTopicProvider($"ParallafkaThroughputTest-{Guid.NewGuid()}");
        }

        [Fact]
        public override Task TestCanConsumeMuchFasterThanDefaultConsumerAsync()
        {
            return base.TestCanConsumeMuchFasterThanDefaultConsumerAsync();
        }
    }
}