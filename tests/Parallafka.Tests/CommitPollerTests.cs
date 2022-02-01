using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests
{
    public class CommitPollerTests
    {
        private readonly ITestOutputHelper _console;
        private readonly Mock<IMessageCommitter> _committer;
        private readonly CommitPoller _poller;

        [Fact]
        public async Task CommitNowAfterDelay()
        {
            // when
            Assert.True(this._poller.CommitWithin(TimeSpan.FromSeconds(1)));
            Assert.True(this._poller.CommitNow());
            this._poller.Complete();
            await this._poller.Completion;

            // then
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task MultipleCommitNowIgnoredDuringLongCommit()
        {
            // when
            this._committer.Setup(m => m.CommitNow(It.IsAny<CancellationToken>()))
                .Returns(() => Task.Delay(1000));
                
            Assert.True(this._poller.CommitNow());
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.True(this._poller.CommitNow());

            // the following CommitNows should be ignored
            Assert.False(this._poller.CommitNow());
            Assert.False(this._poller.CommitNow());
            Assert.False(this._poller.CommitNow());
            Assert.False(this._poller.CommitNow());
            Assert.False(this._poller.CommitNow());
            this._poller.Complete();
            await this._poller.Completion;

            // then
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Fact]
        public async Task CommitNowCallsCommit()
        {
            // when
            Assert.True(this._poller.CommitNow());
            this._poller.Complete();
            await this._poller.Completion;

            // then
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Once);
        }
        
        [Fact]
        public async Task CommitNowBeforeDelayBothCallCommit()
        {
            // when
            Assert.True(this._poller.CommitNow());
            Assert.True(this._poller.CommitWithin(TimeSpan.FromSeconds(1)));
            this._poller.Complete();
            await this._poller.Completion;

            // then
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Fact]
        public async Task CommitWithinWorks()
        {
            // when
            Assert.True(this._poller.CommitWithin(TimeSpan.FromMilliseconds(100)));
            await Task.Delay(TimeSpan.FromSeconds(1));

            // then
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Once);

            this._poller.Complete();
            await this._poller.Completion;
        }


        [Fact]
        public async Task CommitWithinWithLongDelayIgnoresDelay()
        {
            // when
            var sw = Stopwatch.StartNew();
            Assert.True(this._poller.CommitWithin(TimeSpan.FromSeconds(100)));
            this._poller.Complete();
            await this._poller.Completion;

            // then
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1));
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task CommitNowIgnoredAfterCompletion()
        {
            // when
            this._poller.Complete();
            await this._poller.Completion;

            Assert.False(this._poller.CommitNow());

            // then
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Never);
        }
        
        [Fact]
        public async Task CommitWithinIgnoredAfterCompletion()
        {
            // when
            this._poller.Complete();
            await this._poller.Completion;

            Assert.False(this._poller.CommitWithin(TimeSpan.FromSeconds(1)));

            // then
            this._committer.Verify(m => m.CommitNow(It.IsAny<CancellationToken>()), Times.Never);
        }

        public CommitPollerTests(ITestOutputHelper console)
        {
            this._console = console;
            this._committer = new Mock<IMessageCommitter>();
            this._poller = new CommitPoller(this._committer.Object);
        }
    }
}
