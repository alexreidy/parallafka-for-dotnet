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
        [Fact]
        public virtual Task TestGracefulShutdownAsync()
        {
            return this.PublishingTestMessagesContinuouslyAsync(async () =>
            {
                var parallafkaConfig = new ParallafkaConfig<string, string>()
                {
                    MaxConcurrentHandlers = 10,
                    DisposeStrategyProvider = self => new Parallafka<string, string>.GracefulShutdownDisposeStrategy(self),
                };
                await using(var consumer = await this.Topic.GetConsumerAsync("myConsumer"))
                await using(var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig))
                {
                    Task disposeTask = Task.CompletedTask;
                    int nHandlersHanging = 0;
                    var hangTcs = new TaskCompletionSource();

                    var hangingHandlerMessages = new ConcurrentBag<IKafkaMessage<string, string>>();
                    var consumedMessages = new ConcurrentBag<IKafkaMessage<string, string>>();
                    var receivedMessages = new ConcurrentBag<IKafkaMessage<string, string>>();

                    //var verifier = new ConsumptionVerifier();

                    int nConsumed = 0;
                    Task consumeTask = parallafka.ConsumeAsync(async msg =>
                    {
                        receivedMessages.Add(msg);
                        if (Interlocked.Increment(ref nConsumed) >= 120)
                        {
                            hangingHandlerMessages.Add(msg);
                            Interlocked.Increment(ref nHandlersHanging);
                            await hangTcs.Task;
                        }

                        consumedMessages.Add(msg);
                    });

                    while (nHandlersHanging != parallafkaConfig.MaxConcurrentHandlers)
                    {
                        await Task.Delay(100);
                    }

                    var expectedCommittedOffsets = consumedMessages.Select(m => m.Offset).ToHashSet();

                    foreach (var offset in expectedCommittedOffsets)
                    {
                        Assert.Contains(offset, consumer.CommittedOffsets);
                    }
                    foreach (var offset in hangingHandlerMessages.Select(m => m.Offset))
                    {
                        Assert.DoesNotContain(offset, consumer.CommittedOffsets);
                    }

                    disposeTask = parallafka.DisposeAsync().AsTask();

                    // somehow assert the task doesn't complete, that it's waiting for handlers...
                    // maybe just spy on the progress w/ internal props: what it's waiting on internally.
                    // Wait for that to happen here.

                    await Task.Delay(5000);

                    foreach (var offset in expectedCommittedOffsets)
                    {
                        Assert.Contains(offset, consumer.CommittedOffsets);
                    }
                    foreach (var offset in hangingHandlerMessages.Select(m => m.Offset))
                    {
                        Assert.DoesNotContain(offset, consumer.CommittedOffsets);
                    }

                    hangTcs.SetResult();

                    // Timeout
                    await disposeTask;
                    
                    // Timeout
                    await consumeTask;
                }
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