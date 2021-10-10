using System;
using System.Threading.Tasks;
using Parallafka.Tests;
using Parallafka.Tests.Commits;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.IntegrationTests.Commits
{
    public class CommitTests : CommitTestsBase
    {
        private readonly ITestKafkaTopic _topic;

        protected override ITestKafkaTopic Topic => this._topic;

        public CommitTests(ITestOutputHelper output)
        {
            this._topic = new TestKafkaTopicProvider($"ParallafkaCommitTest-{Guid.NewGuid().ToString()}");
        }

        [Fact]
        public override Task MessagesAreNotComittedTillAllEarlierOnesAreHandledAsync()
        {
            return base.MessagesAreNotComittedTillAllEarlierOnesAreHandledAsync();
        }
    }
}