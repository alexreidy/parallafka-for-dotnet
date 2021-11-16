using System;
using System.Threading.Tasks;
using Parallafka.Tests;
using Parallafka.Tests.OrderGuarantee;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.IntegrationTests.OrderGuarantee
{
    public class OrderGuaranteeTests : OrderGuaranteeTestBase
    {
        private readonly TestKafkaTopicProvider _topic;

        protected override ITestKafkaTopic Topic => this._topic;

        public OrderGuaranteeTests(ITestOutputHelper console) : base(console)
        {
            this._topic = new TestKafkaTopicProvider($"ParallafkaOrderGuaranteeTest-{Guid.NewGuid()}");
        }

        [Fact]
        public override Task TestFifoOrderIsPreservedForSameKeyAsync()
        {
            return base.TestFifoOrderIsPreservedForSameKeyAsync();
        }
    }
}