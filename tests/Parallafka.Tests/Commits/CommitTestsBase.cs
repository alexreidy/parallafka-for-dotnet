using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Commits
{
    public abstract class CommitTestsBase : KafkaTopicTestBase
    {
        // TODO: I'd like a test that goes longer over thousands of msgs and multiple random hangs and have a
        // continuous detection assertion system that alerts if any msgs > hung msg are committed before hung msg is finished.

        public virtual async Task MessagesAreNotCommittedTillAllEarlierOnesAreHandledAsync()
        {
            // Parallafka<string, string>.WriteLine = s => this.Console.WriteLine(s);

            var parallafkaConfig = new ParallafkaConfig<string, string>()
            {
                MaxDegreeOfParallelism = 7,
                // TODO: This should be hard-stop dispose strategy as otherwise we may not surface assertion exceptions
            };

            int offsetOfMessageToHang = 12;

            Task<List<IKafkaMessage<string, string>>> publishTask = this.PublishTestMessagesAsync(400, duplicateKeys: true);
            KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync("parallafka");
            var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig);
            {
                var consumed = new ConcurrentQueue<IKafkaMessage<string, string>>();
                var firstPartitionMsgsConsumed = new ConcurrentQueue<IKafkaMessage<string, string>>();
                TaskCompletionSource hangEarlyMsgTcs = new();
                using CancellationTokenSource stopConsuming = new CancellationTokenSource();
                Task consumeTask = parallafka.ConsumeAsync(async msg =>
                {
                    await Task.Delay(StaticRandom.Use(r => r.Next(25)));

                    if (msg.Offset.Partition == 0)
                    {
                        if (msg.Offset.Offset == offsetOfMessageToHang)
                        {
                            await hangEarlyMsgTcs.Task;
                        }

                        firstPartitionMsgsConsumed.Enqueue(msg);
                    }

                    consumed.Enqueue(msg);
                }, stopConsuming.Token);

                List<IKafkaMessage<string, string>> consumedMessagesBeforeHungMsg = new();
                await Wait.UntilAsync(
                    "Consumed and committed the messages in the partition from offset 0 until the first hung message",
                    () =>
                    {
                        long minExpectedConsumedCount = offsetOfMessageToHang - 1;
                        Assert.True(firstPartitionMsgsConsumed.Count >= minExpectedConsumedCount,
                            $"Should have consumed at least {minExpectedConsumedCount} but consumed {firstPartitionMsgsConsumed.Count}");
                        
                        consumedMessagesBeforeHungMsg = new();
                        for (int i = 0; i < offsetOfMessageToHang; i++)
                        {
                            Assert.Contains(i, firstPartitionMsgsConsumed.Select(m => m.Offset.Offset));
                            Assert.Contains(consumer.CommittedOffsets.Where(o => o.Partition == 0).Select(o => o.Offset), offset => offset >= i);
                            consumedMessagesBeforeHungMsg.Add(firstPartitionMsgsConsumed.First(m => m.Offset.Offset == i));
                        }

                        return Task.CompletedTask;
                    },
                    timeout: TimeSpan.FromSeconds(50));

                Assert.Equal(offsetOfMessageToHang, consumedMessagesBeforeHungMsg.Count);
                Assert.True(firstPartitionMsgsConsumed.Count >= offsetOfMessageToHang);

                await Wait.UntilAsync("Consumed some messages in the partition after the hung message",
                    () =>
                    {
                        var messagesConsumedAfterHungMessage = firstPartitionMsgsConsumed.Except(
                            firstPartitionMsgsConsumed.Where(m => m.Offset.Offset <= offsetOfMessageToHang));
                        int nConsumed = messagesConsumedAfterHungMessage.Count();
                        Assert.True(nConsumed > 25, $"Consumed {nConsumed} messages after hung message");

                        return Task.CompletedTask;
                    },
                    timeout: TimeSpan.FromSeconds(33));

                // Give Parallafka a _chance_ to commit after consuming those - but it should not commit the hung message or beyond.
                await Task.Delay(9999);

                // Assert that the hung message and consumed messages beyond have not been committed.
                Assert.DoesNotContain(offsetOfMessageToHang, consumer.CommittedOffsets.Where(o => o.Partition == 0).Select(o => o.Offset));
                foreach (var msg in firstPartitionMsgsConsumed)
                {
                    if (consumedMessagesBeforeHungMsg.Contains(msg))
                    {
                        continue;
                    }
                    Assert.DoesNotContain(msg.Offset.Offset, consumer.CommittedOffsets.Where(o => o.Partition == 0).Select(o => o.Offset));
                }

                hangEarlyMsgTcs.SetResult();

                stopConsuming.Cancel();
                await consumeTask;

                var consumptionVerifier = new ConsumptionVerifier();
                var publishedMsgs = await publishTask;
                consumptionVerifier.AddSentMessages(publishedMsgs);
                consumptionVerifier.AddConsumedMessages(consumed);
                consumptionVerifier.AssertConsumedAllSentMessagesProperly();
                consumptionVerifier.AssertAllConsumedMessagesWereCommitted(consumer);
            }
        }

        protected CommitTestsBase(ITestOutputHelper console) : base(console)
        {
        }
    }
}