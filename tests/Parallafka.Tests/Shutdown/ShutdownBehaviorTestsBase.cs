using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.OrderGuarantee;
using Xunit;

namespace Parallafka.Tests.Shutdown
{
    public abstract class ShutdownBehaviorTestsBase : KafkaTopicTestBase
    {
        public virtual Task TestGracefulShutdownAsync()
        {
            return this.PublishingTestMessagesContinuouslyAsync(async () =>
            {
                var parallafkaConfig = new ParallafkaConfig<string, string>()
                {
                    MaxConcurrentHandlers = 10,
                    DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self),
                };

                var verifier = new ConsumptionVerifier();

                var consumer = await this.Topic.GetConsumerAsync("myConsumer");
                var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig);
                // Not using `using` here in case these hang during disposal.
                {
                    var consumedMessages = new ConcurrentBag<IKafkaMessage<string, string>>();
                    var rngs = new ThreadSafeRandom();
                    Task consumeTask = parallafka.ConsumeAsync(async msg =>
                    {
                        await rngs.BorrowAsync(async rng =>
                        {
                            await Task.Delay(rng.Next(20));
                        });

                        consumedMessages.Add(msg);
                        verifier.AddConsumedMessages(new[] { msg });
                    });

                    await Wait.UntilAsync("Consumed a bunch of messages", async () => consumedMessages.Count > 250,
                        timeout: TimeSpan.FromSeconds(45));

                    Task disposeTask = parallafka.DisposeAsync().AsTask();

                    // TODO: Show that disposal doesn't complete until all in-progress messages are handled and committed.
                    // And demonstrate the timeout works if they remain hanging.

                    await Wait.UntilAsync("All consumed messages are committed",
                        async () =>
                        {
                            Assert.True(disposeTask.IsCompletedSuccessfully);
                            verifier.AssertAllConsumedMessagesWereCommitted(consumer);
                        },
                        timeout: TimeSpan.FromSeconds(30));

                    verifier.AddSentMessages(consumedMessages);
                    verifier.AssertConsumedAllSentMessagesProperly();

                    await Wait.ForTaskOrTimeoutAsync(consumeTask, TimeSpan.FromSeconds(15),
                        onTimeout: () => throw new Exception("Timed out waiting for consumeTask"));

                    await Wait.ForTaskOrTimeoutAsync(disposeTask, TimeSpan.FromSeconds(15),
                        onTimeout: () => throw new Exception("Timed out waiting for disposeTask"));
                }
            });
        }

        public virtual async Task TestHardStopShutdownAsync()
        {
            var parallafkaConfig = new ParallafkaConfig<string, string>()
            {
                MaxConcurrentHandlers = 10,
                DisposeStrategyProvider = self => new Parallafka<string, string>.HardStopDisposeStrategy(self),
            };
            var consumer = await this.Topic.GetConsumerAsync("myConsumer");
            var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig);
            // Not using `using` here in case these hang during disposal.

            await this.PublishingTestMessagesContinuouslyAsync(async () =>
            {
                var consumedMessages = new ConcurrentBag<IKafkaMessage<string, string>>();
                var rngs = new ThreadSafeRandom();
                Task consumeTask = parallafka.ConsumeAsync(async msg =>
                {
                    await rngs.BorrowAsync(async rng =>
                    {
                        await Task.Delay(rng.Next(20));
                    });

                    if (msg.Offset.Partition == 0 && msg.Offset.Offset == 20)
                    {
                        await Task.Delay(TimeSpan.FromDays(365));
                    }

                    consumedMessages.Add(msg);
                });

                await Wait.UntilAsync("Consumed a bunch of messages", async () => consumedMessages.Count > 250,
                    timeout: TimeSpan.FromSeconds(45));

                Task disposeTask = parallafka.DisposeAsync().AsTask();
                await Wait.ForTaskOrTimeoutAsync(disposeTask, TimeSpan.FromSeconds(4),
                    onTimeout: () => throw new Exception("Timed out waiting for disposeTask"));

                // One handler thread is hanging but DisposeAsync should still return almost immediately.

                // TODO: Expand test
            });
        }

        private async Task PublishingTestMessagesContinuouslyAsync(Func<Task> actAsync)
        {
            Task publishTask = Task.CompletedTask;
            CancellationTokenSource publishCts = new();
            try
            {
                publishTask = this.PublishTestMessagesUntilCancelAsync(publishCts.Token);
                await actAsync.Invoke();
            }
            finally
            {
                publishCts.Cancel();
                await publishTask;
            }
        }
    }
}