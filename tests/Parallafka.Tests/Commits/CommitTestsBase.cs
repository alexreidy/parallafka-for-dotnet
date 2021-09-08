#pragma warning disable CS4014

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.OrderGuarantee;
using Xunit;

namespace Parallafka.Tests.Contracts
{
    public abstract class CommitTestsBase : KafkaTopicTestBase
    {
        // TODO: I'd like a test that goes longer over thousands of msgs and multiple random hangs and have a
        // continuous detection assertion system that alerts if any msgs > hung msg are committed before hung msg is finished.

        [Fact]
        public virtual async Task MessagesAreNotComittedTillAllEarlierOnesAreHandledAsync()
        {
            // Hang an earlier message in partition; let newer msgs be handled.
            // Show that nothing finished is committed until earliest is handled.

            var parallafkaConfig = new ParallafkaConfig<string, string>()
            {
                MaxConcurrentHandlers = 7,
                // TODO: This should be hard-stop dispose strategy as otherwise we may not surface assertion exceptions
            };

            int offsetOfMessageToHang = 12;

            Task<IEnumerable<IKafkaMessage<string, string>>> publishTask = this.PublishTestMessagesAsync(400, duplicateKeys: false);
            await using(KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync("parallafka"))
            await using(var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig))
            {
                var consumed = new ConcurrentQueue<IKafkaMessage<string, string>>();
                var firstPartitionMsgsConsumed = new ConcurrentQueue<IKafkaMessage<string, string>>();
                
                TaskCompletionSource hangEarlyMsgTcs = new();
                TaskCompletionSource hangSubsequentMsgsTcs = new();
                int nHandlersHanging = 0;

                var msgsBeingHandled = new ConcurrentDictionary<IKafkaMessage<string, string>, IKafkaMessage<string, string>>();

                var rngs = new ThreadSafeRandom();
                Task consumeTask = parallafka.ConsumeAsync(async msg =>
                {
                    await rngs.BorrowAsync(async rng =>
                    {
                        await Task.Delay(rng.Next(25));
                    });

                    msgsBeingHandled[msg] = msg;
                    if (msg.Offset.Partition == 0)
                    {
                        if (msg.Offset.Offset == offsetOfMessageToHang)
                        {
                            Interlocked.Increment(ref nHandlersHanging);
                            await hangEarlyMsgTcs.Task;
                            Interlocked.Decrement(ref nHandlersHanging);
                        }
                        else
                        {
                            // TODO: Why did I have these hang too?
                            if (msg.Offset.Offset > offsetOfMessageToHang)
                            {
                                Interlocked.Increment(ref nHandlersHanging);
                                await hangSubsequentMsgsTcs.Task;
                                Interlocked.Decrement(ref nHandlersHanging);
                            }
                        }

                        firstPartitionMsgsConsumed.Enqueue(msg);
                    }

                    consumed.Enqueue(msg);
                    msgsBeingHandled.TryRemove(msg, out var _);
                });

                await Wait.UntilAsync("All handlers are hanging",
                    async () =>
                    {
                        Assert.Equal(parallafkaConfig.MaxConcurrentHandlers, nHandlersHanging);
                    },
                    timeout: TimeSpan.FromSeconds(60));

                int nConsumedInPartition = 0;
                await Wait.UntilAsync(
                    "Consumed the number of messages in the partition from offset 0 until the first hung message",
                    async () =>
                    {
                        Assert.Equal((nConsumedInPartition = consumed.Where(msg => msg.Offset.Partition == 0).Count()), offsetOfMessageToHang);
                    },
                    timeout: TimeSpan.FromSeconds(15));

                Assert.Equal(nConsumedInPartition, offsetOfMessageToHang);
                await Task.Delay(2000);

                await Wait.UntilAsync(
                    "Messages in the partition from offset 0 up to the first hung msg have been committed",
                    async () =>
                    {
                        Assert.Equal(nConsumedInPartition, offsetOfMessageToHang);
                        Assert.Equal(nConsumedInPartition, firstPartitionMsgsConsumed.Count);
                        foreach (var message in firstPartitionMsgsConsumed)
                        {
                            Assert.Contains(message.Offset, consumer.CommittedOffsets);
                        }
                    },
                    timeout: TimeSpan.FromSeconds(15));

                Assert.Equal(parallafkaConfig.MaxConcurrentHandlers, msgsBeingHandled.Count);
                var msgsThatWereHanging = new HashSet<IKafkaMessage<string, string>>();
                foreach (var msg in msgsBeingHandled.Values)
                {
                    msgsThatWereHanging.Add(msg);
                    Assert.DoesNotContain(msg.Offset, consumer.CommittedOffsets);
                }

                // Let handlers continue, except the one stuck on the first hung message.
                hangSubsequentMsgsTcs.SetResult();

                IEnumerable<IKafkaMessage<string, string>> formerlyHangingMsgs = msgsThatWereHanging.Where(m => m.Offset.Offset != offsetOfMessageToHang);
                await Wait.UntilAsync("Formerly hanging messages following the stuck message have been consumed",
                    async () =>
                    {
                        foreach (var msg in formerlyHangingMsgs)
                        {
                            Assert.Contains(msg, consumed);
                        }
                    },
                    timeout: TimeSpan.FromSeconds(15));

                await Task.Delay(5000);
                foreach (var msg in formerlyHangingMsgs)
                {
                    Assert.DoesNotContain(msg.Offset, consumer.CommittedOffsets);
                }

                hangEarlyMsgTcs.SetResult();

                await Wait.UntilAsync("Everything sent was consumed and committed", async () =>
                {
                    var consumptionVerifier = new ConsumptionVerifier();
                    var publishedMsgs = await publishTask;
                    consumptionVerifier.AddSentMessages(publishedMsgs);
                    consumptionVerifier.AddConsumedMessages(consumed);
                    consumptionVerifier.AssertConsumedAllSentMessagesProperly();
                    consumptionVerifier.AssertAllConsumedMessagesWereCommitted(consumer);
                },
                timeout: TimeSpan.FromSeconds(20));
            }
        }
    }
}