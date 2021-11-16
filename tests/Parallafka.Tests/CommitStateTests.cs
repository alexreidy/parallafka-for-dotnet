using System.Linq;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests
{
    public class CommitStateTests
    {
        [Fact]
        public void QueueStateIsCorrect()
        {
            //  given
            var cs = new CommitState<string, string>();
            var km = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 0));
            Assert.False(cs.TryGetMessageToCommit(km, out _));

            // when
            cs.EnqueueMessage(km);

            // then
            Assert.False(cs.TryGetMessageToCommit(km, out _));

            // when
            km.WasHandled = true;

            // then
            Assert.True(cs.TryGetMessageToCommit(km, out _));
            Assert.False(cs.TryGetMessageToCommit(km, out _));
        }

        [Fact]
        public void QueueStateForSeveralOffsetsIsCorrect()
        {
            // given
            var cs = new CommitState<string, string>();
            var kms = Enumerable.Range(1, 5).Select(i =>
                new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i))).ToList();

            // when
            foreach (var km in kms)
            {
                cs.EnqueueMessage(km);
            }

            IKafkaMessage<string, string> messageToCommit;

            // then
            foreach (var km in kms)
            {
                Assert.False(cs.TryGetMessageToCommit(km, out messageToCommit));
            }

            // when
            kms[2].WasHandled = true;

            // then
            foreach (var km in kms)
            {
                Assert.False(cs.TryGetMessageToCommit(km, out messageToCommit));
            }

            // when
            kms[0].WasHandled = true;
            kms[1].WasHandled = true;

            // then
            Assert.True(cs.TryGetMessageToCommit(kms[2], out messageToCommit));
            Assert.Equal(kms[2], messageToCommit);
        }

        [Fact]
        public void NoMessagesToCommitWhenNoMessagesHandled()
        {
            // given
            var cs = new CommitState<string, string>();
            var kms = Enumerable.Range(1, 5).Select(i =>
                new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i))).ToList();

            // when
            foreach (var km in kms)
            {
                cs.EnqueueMessage(km);
            }

            // then
            Assert.Empty(cs.GetMessagesToCommit());
        }
        
        [Fact]
        public void LatestMessageToCommitWhenAllMessagesHandled()
        {
            // given
            var cs = new CommitState<string, string>();
            var kms = Enumerable.Range(1, 5).Select(i =>
                new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i))).ToList();

            foreach (var km in kms)
            {
                km.WasHandled = true;
                cs.EnqueueMessage(km);
            }

            // when
            var messagesToCommit = cs.GetMessagesToCommit();

            // then
            Assert.Equal(new RecordOffset(0, 5), messagesToCommit.Single().Offset);
        }

        [Theory]
        [InlineData(1,1)]
        [InlineData(1,5)]
        [InlineData(3,5)]
        [InlineData(12,65)]
        public void LatestMessageFromEachPartitionToCommitWhenAllMessagesHandled(int partitions, int messages)
        {
            // given
            var cs = new CommitState<string, string>();
            var kms =
                Enumerable.Range(1, partitions).SelectMany(p =>
                    Enumerable.Range(1, messages).Select(i =>
                        new KafkaMessage<string, string>("key", "value", new RecordOffset(p, i)))).ToList();

            foreach (var km in kms)
            {
                km.WasHandled = true;
                cs.EnqueueMessage(km);
            }

            // when
            var messagesToCommit = cs.GetMessagesToCommit().ToArray();

            // then
            Assert.Equal(partitions, messagesToCommit.Length);
            foreach (var p in Enumerable.Range(1, partitions))
            {
                Assert.Equal(new RecordOffset(p, messages), messagesToCommit[p-1].Offset);
            }
        }
    }
}
