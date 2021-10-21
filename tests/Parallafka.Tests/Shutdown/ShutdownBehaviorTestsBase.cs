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

        public virtual async Task TestGracefulShutdownTimeoutAsync()
        {
            var shutdownTimeout = TimeSpan.FromSeconds(12);
            var timeoutWithExceptionConfig = new ParallafkaConfig<string, string>()
            {
                MaxConcurrentHandlers = 10,
                DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self,
                    waitTimeout: shutdownTimeout, throwExceptionOnTimeout: true),
            };
            var timeoutWithoutExceptionConfig = new ParallafkaConfig<string, string>()
            {
                MaxConcurrentHandlers = 10,
                DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self,
                    waitTimeout: shutdownTimeout, throwExceptionOnTimeout: false),
            };
            var noTimeoutConfig = new ParallafkaConfig<string, string>()
            {
                MaxConcurrentHandlers = 10,
                DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self,
                    waitTimeout: null),
            };
            var consumer1 = await this.Topic.GetConsumerAsync("timeoutWithException");
            var consumer2 = await this.Topic.GetConsumerAsync("timeoutWithoutException");
            var consumer3 = await this.Topic.GetConsumerAsync("noTimeoutEvenAtAllWhatsoever");
            var timeoutWithExceptionInstance = new Parallafka<string, string>(
                consumer1, timeoutWithExceptionConfig);
            var timeoutWithoutExceptionInstance = new Parallafka<string, string>(
                consumer2, timeoutWithoutExceptionConfig);
            var noTimeoutInstance = new Parallafka<string, string>(
                consumer3, noTimeoutConfig);
            {
                var verifier1 = new ConsumptionVerifier();
                var verifier2 = new ConsumptionVerifier();
                var verifier3 = new ConsumptionVerifier();
                var finalMessage = new KafkaMessage<string, string>("TheFinalMessage", "I am the walrus");
                int nhandlersHangingOnFinalMsg = 0;
                var rngs = new ThreadSafeRandom();

                Func<IKafkaMessage<string, string>, Task> handlerWithVerifier(ConsumptionVerifier verifier)
                {
                    return async msg =>
                    {
                        if (msg.Key == finalMessage.Key)
                        {
                            Interlocked.Increment(ref nhandlersHangingOnFinalMsg);
                            await Task.Delay(TimeSpan.FromMinutes(3));
                        }
                        else
                        {
                            await rngs.BorrowAsync(rng => Task.Delay(rng.Next(50)));
                        }
                        verifier.AddConsumedMessage(msg);
                    };
                }

                Task consumeTask1 = timeoutWithExceptionInstance.ConsumeAsync(handlerWithVerifier(verifier1));
                Task consumeTask2 = timeoutWithoutExceptionInstance.ConsumeAsync(handlerWithVerifier(verifier2));
                Task consumeTask3 = noTimeoutInstance.ConsumeAsync(handlerWithVerifier(verifier3));

                var publishedMsgs = await this.PublishTestMessagesAsync(42);
                verifier1.AddSentMessages(publishedMsgs);
                verifier2.AddSentMessages(publishedMsgs);
                verifier3.AddSentMessages(publishedMsgs);
                await Wait.UntilAsync("Consumed initial batch", async () =>
                {
                    verifier1.AssertConsumedAllSentMessagesProperly();
                    verifier1.AssertAllConsumedMessagesWereCommitted(consumer1);
                    verifier2.AssertConsumedAllSentMessagesProperly();
                    verifier2.AssertAllConsumedMessagesWereCommitted(consumer2);
                    verifier3.AssertConsumedAllSentMessagesProperly();
                    verifier3.AssertAllConsumedMessagesWereCommitted(consumer3);
                },
                timeout: TimeSpan.FromSeconds(30));

                await this.Topic.PublishAsync(new[] { finalMessage });
                await Wait.UntilAsync("All handlers are hanging on final msg",
                    async () => nhandlersHangingOnFinalMsg == 3,
                    timeout: TimeSpan.FromSeconds(7));

                // Kick this off. We'll revisit and verify it hasn't completed.
                Task disposeWithoutTimeoutTask = noTimeoutInstance.DisposeAsync().AsTask();

                var shutdownStartTime = DateTime.UtcNow;
                await Assert.ThrowsAsync<TimeoutException>(async () =>
                {
                    Task disposeTask = timeoutWithExceptionInstance.DisposeAsync().AsTask();

                    var metaTimeoutTask = Task.Delay(shutdownTimeout.Add(TimeSpan.FromSeconds(5)));
                    await Task.WhenAny(disposeTask, metaTimeoutTask);
                    if (metaTimeoutTask.IsCompleted)
                    {
                        throw new Exception("Timed out waiting for Parallafka's disposal to time out");
                    }
                    await disposeTask;
                });
                Assert.True(DateTime.UtcNow - shutdownStartTime >= shutdownTimeout);

                shutdownStartTime = DateTime.UtcNow;
                Task disposeTask = timeoutWithoutExceptionInstance.DisposeAsync().AsTask();
                var metaTimeoutTask = Task.Delay(shutdownTimeout.Add(TimeSpan.FromSeconds(5)));
                if (metaTimeoutTask.IsCompleted)
                {
                    throw new Exception("Timed out waiting for Parallafka's disposal to time out");
                }
                // DisposeAsync() should not throw.
                await disposeTask;

                await Task.Delay(shutdownTimeout.Add(TimeSpan.FromSeconds(12)));
                Assert.False(disposeWithoutTimeoutTask.IsCompleted);
            }
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