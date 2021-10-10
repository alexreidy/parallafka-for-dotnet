
using System;
using System.Threading.Tasks;
using Parallafka.Tests;
using Parallafka.Tests.Shutdown;
using Xunit;

namespace Parallafka.IntegrationTests.Shutdown
{
    public class ShutdownBehaviorTests : ShutdownBehaviorTestsBase
    {
        private TestKafkaTopicProvider _topic;
        protected override ITestKafkaTopic Topic => this._topic;

        public ShutdownBehaviorTests()
        {
            this._topic = new TestKafkaTopicProvider($"ParallafkaShutdownBehaviorTest-{Guid.NewGuid().ToString()}");
        }

        [Fact]
        public override Task TestGracefulShutdownAsync()
        {
            return base.TestGracefulShutdownAsync();
        }

        [Fact]
        public override Task TestHardStopShutdownAsync()
        {
            return base.TestHardStopShutdownAsync();
        }
    }
}