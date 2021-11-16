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
        private readonly RealKafkaTopicProvider _topic = new($"ParallafkaThroughputTest-{Guid.NewGuid()}");

        protected override ITestKafkaTopic Topic => this._topic;

        [Fact]
        public override Task TestCanConsumeMuchFasterThanDefaultConsumerAsync()
        {
            return base.TestCanConsumeMuchFasterThanDefaultConsumerAsync();
        }

        public ThroughputTests(ITestOutputHelper output) : base(output)
        {
        }
    }
}