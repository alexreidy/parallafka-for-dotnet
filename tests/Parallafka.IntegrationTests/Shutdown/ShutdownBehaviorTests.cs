using System;
using System.Threading.Tasks;
using Parallafka.Tests;
using Parallafka.Tests.Shutdown;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.IntegrationTests.Shutdown
{
    public class ShutdownBehaviorTests : ShutdownBehaviorTestsBase
    {
        private readonly RealKafkaTopicProvider _topic = new($"ParallafkaShutdownBehaviorTest-{Guid.NewGuid()}");

        protected override ITestKafkaTopic Topic => this._topic;

        [Fact]
        public override Task TestGracefulShutdownAsync()
        {
            return base.TestGracefulShutdownAsync();
        }

        public ShutdownBehaviorTests(ITestOutputHelper console) : base(console)
        {
        }
    }
}