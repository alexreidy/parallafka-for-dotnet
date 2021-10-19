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
        public virtual async Task TestGracefulShutdownAsync()
        {
            await this.PublishingTestMessagesContinuouslyAsync(async () =>
            {
                var parallafkaConfig = new ParallafkaConfig<string, string>()
                {
                    MaxConcurrentHandlers = 10,
                    DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self),
                };

                var verifier = new ConsumptionVerifier();
                Func<Task> maybeHangAsync = () => Task.CompletedTask;
                int nMessagesBeingHandled = 0;

                bool trackConsumedMessages = true;

                var consumer = await this.Topic.GetConsumerAsync("myConsumer");
                var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig);
                // Not using `using` here in case these hang during disposal.
                {
                    var consumedMessages = new ConcurrentBag<IKafkaMessage<string, string>>();
                    var rngs = new ThreadSafeRandom();
                    Task consumeTask = parallafka.ConsumeAsync(async msg =>
                    {
                        Interlocked.Increment(ref nMessagesBeingHandled);
                        await rngs.BorrowAsync(async rng =>
                        {
                            await Task.Delay(rng.Next(20));
                        });

                        await maybeHangAsync();

                        if (trackConsumedMessages)
                        {
                            consumedMessages.Add(msg);
                            verifier.AddConsumedMessages(new[] { msg });
                        }

                        Interlocked.Decrement(ref nMessagesBeingHandled);
                    });

                    await Wait.UntilAsync("Consumed a bunch of messages", async () => consumedMessages.Count > 250,
                        timeout: TimeSpan.FromSeconds(45));

                    var hangHandlerTcs = new TaskCompletionSource();
                    maybeHangAsync = () => hangHandlerTcs.Task;
                    // For thread safety, hang all handlers before commencing the shutdown
                    await Wait.UntilAsync("All handlers are hanging",
                        async () => nMessagesBeingHandled == parallafkaConfig.MaxConcurrentHandlers, 
                        timeout: TimeSpan.FromSeconds(9));
                    
                    // Stop tracking consumed messages before initiating shutdown
                    // so we know the set that should definitely be committed.
                    trackConsumedMessages = false;

                    Task disposeTask = parallafka.DisposeAsync().AsTask();

                    hangHandlerTcs.SetResult();

                    await Wait.UntilAsync("Parallafka instance is disposed",
                        async () =>
                        {
                            Assert.True(disposeTask.IsCompletedSuccessfully);
                        },
                        timeout: TimeSpan.FromSeconds(60));

                    Assert.Equal(0, nMessagesBeingHandled);

                    await Wait.UntilAsync("All messages consumed pre-shutdown request are committed",
                        async () =>
                        {
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

        // public virtual async Task TestGracefulShutdownTimeoutAsync()
        // {
        //     var parallafkaConfig = new ParallafkaConfig<string, string>()
        //     {
        //         MaxConcurrentHandlers = 10,
        //         DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self),
        //     };

        //     As
        // }

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