﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests
{
    public class MessageCommitterTests
    {
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CommitsSimpleRecordAsync(bool wasHandled)
        {
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>();
            var kafkaMessage = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 0));
            commitState.EnqueueMessage(kafkaMessage);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object,
                TimeSpan.FromDays(1),
                default);
            kafkaMessage.WasHandled = wasHandled;

            // when
            await mc.TryCommitMessage(kafkaMessage);

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IRecordOffset>(r => r.Offset == 0 && r.Partition == 0)),
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
            var commitState = new CommitState<string, string>();
            var kafkaMessage1 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 1));
            var kafkaMessage2 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 2));
            var kafkaMessage3 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 3));
            commitState.EnqueueMessage(kafkaMessage1);
            commitState.EnqueueMessage(kafkaMessage2);
            commitState.EnqueueMessage(kafkaMessage3);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object,
                TimeSpan.FromDays(1),
                default);
            kafkaMessage1.WasHandled = true;
            kafkaMessage2.WasHandled = true;
            kafkaMessage3.WasHandled = false;

            // when
            await mc.TryCommitMessage(kafkaMessage3);

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IRecordOffset>(r => r.Equals(kafkaMessage2.Offset))), Times.Once);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }
        
        [Fact]
        public async Task CommitsLatestCorrectlyAsync()
        {
            // given
            var consumer = new Mock<IKafkaConsumer<string, string>>();
            var logger = new Mock<ILogger>();
            var commitState = new CommitState<string, string>();
            var kafkaMessage1 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 1));
            var kafkaMessage2 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 2));
            var kafkaMessage3 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 3));
            commitState.EnqueueMessage(kafkaMessage1);
            commitState.EnqueueMessage(kafkaMessage2);
            commitState.EnqueueMessage(kafkaMessage3);
            var mc = new MessageCommitter<string, string>(
                consumer.Object,
                commitState,
                logger.Object,
                TimeSpan.FromDays(1),
                default);
            kafkaMessage1.WasHandled = true;
            kafkaMessage2.WasHandled = true;
            kafkaMessage3.WasHandled = true;

            // when
            await mc.TryCommitMessage(kafkaMessage3);

            // then
            consumer.Verify(c => c.CommitAsync(It.Is<IRecordOffset>(r => r.Equals(kafkaMessage3.Offset))), Times.Once);
            consumer.VerifyNoOtherCalls();

            Assert.Empty(commitState.GetMessagesToCommit());
        }
    }
}