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
        private readonly RealKafkaTopicProvider _topic = new($"ParallafkaOrderGuaranteeTest-{Guid.NewGuid()}");

        protected override ITestKafkaTopic Topic => this._topic;

        [Fact]
        public override Task TestFifoOrderIsPreservedForSameKeyAsync()
        {
            return base.TestFifoOrderIsPreservedForSameKeyAsync();
        }

        public OrderGuaranteeTests(ITestOutputHelper console) : base(console)
        {
        }
    }
}