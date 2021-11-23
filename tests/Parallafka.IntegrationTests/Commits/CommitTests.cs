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
        private readonly RealKafkaTopicProvider _topic = new($"ParallafkaCommitTest-{Guid.NewGuid()}");

        protected override ITestKafkaTopic Topic => this._topic;

        [Fact]
        public override Task MessagesAreNotCommittedTillAllEarlierOnesAreHandledAsync()
        {
            return base.MessagesAreNotCommittedTillAllEarlierOnesAreHandledAsync();
        }

        public CommitTests(ITestOutputHelper console) : base(console)
        {
        }
    }
}