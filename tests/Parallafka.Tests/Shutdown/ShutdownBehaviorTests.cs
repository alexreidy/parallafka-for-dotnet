using System.Threading.Tasks;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Shutdown
{
    public class ShutdownBehaviorTests : ShutdownBehaviorTestsBase
    {
        protected override ITestKafkaTopic Topic { get; }

        public ShutdownBehaviorTests(ITestOutputHelper console) : base(console)
        {
            this.Topic = new FakeTestKafkaTopic();
        }

        [Fact]
        public override Task TestGracefulShutdownAsync()
        {
            return base.TestGracefulShutdownAsync();
        }
    }
}
