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
        private readonly TestKafkaTopicProvider _topic;

        protected override ITestKafkaTopic Topic => this._topic;

        public ShutdownBehaviorTests(ITestOutputHelper console) : base(console)
        {
            this._topic = new TestKafkaTopicProvider($"ParallafkaShutdownBehaviorTest-{Guid.NewGuid()}");
        }

        [Fact]
        public override Task TestGracefulShutdownAsync()
        {
            return base.TestGracefulShutdownAsync();
        }
    }
}