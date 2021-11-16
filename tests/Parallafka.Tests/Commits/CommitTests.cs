using System.Threading.Tasks;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Commits
{
    public class CommitTests : CommitTestsBase
    {
        public CommitTests(ITestOutputHelper console) : base(console)
        {
            this.Topic = new FakeTestKafkaTopic();
        }

        protected override ITestKafkaTopic Topic { get; }

        [Fact]
        public override Task MessagesAreNotCommittedTillAllEarlierOnesAreHandledAsync()
        {
            return base.MessagesAreNotCommittedTillAllEarlierOnesAreHandledAsync();
        }
    }
}
