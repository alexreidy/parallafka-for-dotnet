using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests
{
    public class CommitStateTests
    {
        [Fact]
        public async Task EnqueuesOnlyUpToMax()
        {
            //  given
            using var stop = new CancellationTokenSource();
            var cs = new CommitState<string, string>(5, stop.Token);
            var messages = Enumerable.Range(0, 6)
                .Select(i => new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i)))
                .ToList();

            foreach (var message in messages.Take(5))
            {
                await cs.EnqueueMessageAsync(message);
            }

            var message6 = messages.Last();
            using var waitToken = new CancellationTokenSource(TimeSpan.FromSeconds(1));

            // when/then
            var enqueueTask = cs.EnqueueMessageAsync(message6);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                var task = await Task.WhenAny(enqueueTask, Task.Delay(-1, waitToken.Token));
                await task;
            });

            // when
            var message1 = messages.First();
            message1.WasHandled = true;

            // then
            var list = cs.GetMessagesToCommit().ToList();

            Assert.NotEmpty(list);

            await Wait.ForTaskOrTimeoutAsync(enqueueTask, TimeSpan.FromSeconds(10),
                () => throw new Exception("Timed out waiting for enqueueTask"));
        }


        [Fact]
        public async Task QueueStateIsCorrect()
        {
            //  given
            var cs = new CommitState<string, string>(int.MaxValue, default);
            var km = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 0));
            Assert.Empty(cs.GetMessagesToCommit());

            // when
            await cs.EnqueueMessageAsync(km);

            // then
            Assert.Empty(cs.GetMessagesToCommit());

            // when
            km.WasHandled = true;

            // then
            Assert.NotEmpty(cs.GetMessagesToCommit());
            Assert.Empty(cs.GetMessagesToCommit());
        }

        [Fact]
        public async Task QueueStateForSeveralOffsetsIsCorrect()
        {
            // given
            var cs = new CommitState<string, string>(int.MaxValue, default);
            var kms = Enumerable.Range(1, 5).Select(i =>
                new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i))).ToList();

            // when
            foreach (var km in kms)
            {
                await cs.EnqueueMessageAsync(km);
            }

            IKafkaMessage<string, string> messageToCommit;

            // then
            Assert.Empty(cs.GetMessagesToCommit());

            // when
            kms[2].WasHandled = true;

            // then
            Assert.Empty(cs.GetMessagesToCommit());

            // when
            kms[0].WasHandled = true;
            kms[1].WasHandled = true;

            // then
            var messagesToCommit = cs.GetMessagesToCommit().ToList();
            Assert.Single(messagesToCommit);
            Assert.Equal(kms[2], messagesToCommit[0]);
        }

        [Fact]
        public async Task NoMessagesToCommitWhenNoMessagesHandled()
        {
            // given
            var cs = new CommitState<string, string>(int.MaxValue, default);
            var kms = Enumerable.Range(1, 5).Select(i =>
                new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i))).ToList();

            // when
            foreach (var km in kms)
            {
                await cs.EnqueueMessageAsync(km);
            }

            // then
            Assert.Empty(cs.GetMessagesToCommit());
        }
        
        [Fact]
        public async Task LatestMessageToCommitWhenAllMessagesHandled()
        {
            // given
            var cs = new CommitState<string, string>(int.MaxValue, default);
            var kms = Enumerable.Range(1, 5).Select(i =>
                new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i))).ToList();

            foreach (var km in kms)
            {
                km.WasHandled = true;
                await cs.EnqueueMessageAsync(km);
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
        public async Task LatestMessageFromEachPartitionToCommitWhenAllMessagesHandled(int partitions, int messages)
        {
            // given
            var cs = new CommitState<string, string>(int.MaxValue, default);
            var kms =
                Enumerable.Range(1, partitions).SelectMany(p =>
                    Enumerable.Range(1, messages).Select(i =>
                        new KafkaMessage<string, string>("key", "value", new RecordOffset(p, i)))).ToList();

            foreach (var km in kms)
            {
                km.WasHandled = true;
                await cs.EnqueueMessageAsync(km);
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
