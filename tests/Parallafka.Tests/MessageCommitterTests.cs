using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Parallafka.KafkaConsumer;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests
{
    public class MessageCommitterTests
    {
        private readonly ITestOutputHelper _output;

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CommitsSimpleRecordAsync(bool wasHandled)
        {
            // Parallafka<string, string>.WriteLine = s => this._output.WriteLine(s);
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>(int.MaxValue, default);
            var kafkaMessage = KafkaMessage.Create("key", "value", new RecordOffset(0, 0)).Wrapped();
            await commitState.EnqueueMessageAsync(kafkaMessage);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object);
            if (wasHandled)
            {
                kafkaMessage.SetIsReadyToCommit();
            }

            // when
            await mc.CommitNow(default);

            this._output.WriteLine("Wait mc.CommitNow finished");

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IKafkaMessage<string, string>>(r => r.Offset.Offset == 0 && r.Offset.Partition == 0)),
                wasHandled
                    ? Times.Once
                    : Times.Never);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }

        [Fact]
        public async Task CommitsOutOfOrderCorrectlyAsync()
        {
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>(int.MaxValue, default);
            var kafkaMessage1 = KafkaMessage.Create("key", "value", new RecordOffset(0, 1)).Wrapped();
            var kafkaMessage2 = KafkaMessage.Create("key", "value", new RecordOffset(0, 2)).Wrapped();
            var kafkaMessage3 = KafkaMessage.Create("key", "value", new RecordOffset(0, 3)).Wrapped();
            await commitState.EnqueueMessageAsync(kafkaMessage1);
            await commitState.EnqueueMessageAsync(kafkaMessage2);
            await commitState.EnqueueMessageAsync(kafkaMessage3);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object);
            kafkaMessage1.SetIsReadyToCommit();
            kafkaMessage2.SetIsReadyToCommit();

            // when
            await mc.CommitNow(default);

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IKafkaMessage<string, string>>(r => r.Offset.Equals(kafkaMessage2.Offset))), Times.Once);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }
        
        [Fact]
        public async Task CommitsLatestCorrectlyAsync()
        {
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>(int.MaxValue, default);
            var kafkaMessage1 = KafkaMessage.Create("key", "value", new RecordOffset(0, 1)).Wrapped();
            var kafkaMessage2 = KafkaMessage.Create("key", "value", new RecordOffset(0, 2)).Wrapped();
            var kafkaMessage3 = KafkaMessage.Create("key", "value", new RecordOffset(0, 3)).Wrapped();
            await commitState.EnqueueMessageAsync(kafkaMessage1);
            await commitState.EnqueueMessageAsync(kafkaMessage2);
            await commitState.EnqueueMessageAsync(kafkaMessage3);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object);
            kafkaMessage1.SetIsReadyToCommit();
            kafkaMessage2.SetIsReadyToCommit();
            kafkaMessage3.SetIsReadyToCommit();

            // when
            await mc.CommitNow(default);

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IKafkaMessage<string, string>>(r => r.Offset.Equals(kafkaMessage3.Offset))), Times.Once);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }

        public MessageCommitterTests(ITestOutputHelper output)
        {
            this._output = output;
        }
    }
}
