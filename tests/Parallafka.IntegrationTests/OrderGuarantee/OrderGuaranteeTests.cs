
using System;
using System.Threading.Tasks;
using Parallafka.Tests;
using Parallafka.Tests.OrderGuarantee;
using Xunit;

namespace Parallafka.IntegrationTests.OrderGuarantee
{
    public class OrderGuaranteeTests : OrderGuaranteeTestBase
    {
        private TestKafkaTopicProvider _topic;
        protected override ITestKafkaTopic Topic => this._topic;

        public OrderGuaranteeTests()
        {
            this._topic = new TestKafkaTopicProvider($"ParallafkaOrderGuaranteeTest-{Guid.NewGuid().ToString()}");
        }

        [Fact]
        public override Task TestFifoOrderIsPreservedForSameKeyAsync()
        {
            return base.TestFifoOrderIsPreservedForSameKeyAsync();
        }
    }
}