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

                    await Wait.UntilAsync("All consumed messages are committed",
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

        // [Fact]
        // public virtual Task TestGracefulShutdownAsync()
        // {
        //     // TODO: Use the same CommitTest code to verify commit behavior here.

        //     return this.PublishingTestMessagesContinuouslyAsync(async () =>
        //     {
        //         var parallafkaConfig = new ParallafkaConfig<string, string>()
        //         {
        //             MaxConcurrentHandlers = 10,
        //             DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self),
        //         };
        //         //await using(var consumer = await this.Topic.GetConsumerAsync("myConsumer"))
        //         //await using(var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig)) todo
        //         var consumer = await this.Topic.GetConsumerAsync("myConsumer");
        //         var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig);
        //         {
        //             Task disposeTask = Task.CompletedTask;
        //             int nHandlersHanging = 0;
        //             var hangTcs = new TaskCompletionSource();

        //             var hangingHandlerMessages = new ConcurrentBag<IKafkaMessage<string, string>>();
        //             var consumedMessages = new ConcurrentBag<IKafkaMessage<string, string>>();

        //             //var verifier = new ConsumptionVerifier();

        //             var rngs = new ThreadSafeRandom();

        //             int nConsumed = 0;
        //             Task consumeTask = parallafka.ConsumeAsync(async msg =>
        //             {
        //                 await rngs.BorrowAsync(async rng =>
        //                 {
        //                     await Task.Delay(rng.Next(15));
        //                 });
                        
        //                 if (Interlocked.Increment(ref nConsumed) >= 120)
        //                 {
        //                     hangingHandlerMessages.Add(msg);
        //                     Interlocked.Increment(ref nHandlersHanging);
        //                     await hangTcs.Task;
        //                 }

        //                 consumedMessages.Add(msg);
        //             });

        //             await Wait.UntilAsync("All handlers are hanging",
        //                 async () => Assert.Equal(parallafkaConfig.MaxConcurrentHandlers, nHandlersHanging),
        //                 timeout: TimeSpan.FromSeconds(66));

        //             await Wait.UntilAsync("All expected offsets have been committed",
        //                 async () =>
        //                 {
        //                     var expectedCommittedOffsets = consumedMessages.Select(m => m.Offset).ToHashSet();

        //                     foreach (var offset in expectedCommittedOffsets)
        //                     {
        //                         Assert.Contains(offset, consumer.CommittedOffsets);
        //                     }
        //                 },
        //                 timeout: TimeSpan.FromSeconds(10));
                    

        //             foreach (var offset in hangingHandlerMessages.Select(m => m.Offset))
        //             {
        //                 Assert.DoesNotContain(offset, consumer.CommittedOffsets);
        //             }

        //             disposeTask = parallafka.DisposeAsync().AsTask();

        //             // somehow assert the task doesn't complete, that it's waiting for handlers...
        //             // maybe just spy on the progress w/ internal props: what it's waiting on internally.
        //             // Wait for that to happen here.

        //             hangTcs.SetResult();

        //             // sometimes hangs todo. when there are no msgs in _handledMessagesNotYetCommitted but tons in partition... ready for commits: _messagesNotYetCommittedByPartition

        //             await Wait.ForTaskOrTimeoutAsync(consumeTask, TimeSpan.FromSeconds(15),
        //                 onTimeout: () => throw new Exception("Timed out waiting for consumeTask"));

        //             await Wait.ForTaskOrTimeoutAsync(disposeTask, TimeSpan.FromSeconds(15),
        //                 onTimeout: () => throw new Exception("Timed out waiting for disposeTask"));
        //         }
        //     });
        // }

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