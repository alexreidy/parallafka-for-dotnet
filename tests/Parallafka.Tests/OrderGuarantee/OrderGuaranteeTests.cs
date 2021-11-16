using System.Threading.Tasks;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.OrderGuarantee
{
    public class OrderGuaranteeTests : OrderGuaranteeTestBase
    {
        protected override ITestKafkaTopic Topic { get; }

        public OrderGuaranteeTests(ITestOutputHelper console) : base(console)
        {
            this.Topic = new FakeTestKafkaTopic();
        }

        [Fact]
        public override Task TestFifoOrderIsPreservedForSameKeyAsync()
        {
            return base.TestFifoOrderIsPreservedForSameKeyAsync();
        }
    }
}
