using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.OrderGuarantee;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Shutdown
{
    public abstract class ShutdownBehaviorTestsBase : KafkaTopicTestBase
    {
        protected ShutdownBehaviorTestsBase(ITestOutputHelper console) : base(console)
        {
        }

        public virtual async Task TestGracefulShutdownAsync()
        {
            // Parallafka<string, string>.WriteLine = s => this.Console.WriteLine(s);
            await this.PublishingTestMessagesContinuouslyAsync(async () =>
            {
                var parallafkaConfig = new ParallafkaConfig<string, string>()
                {
                    MaxDegreeOfParallelism = 10
                };

                var verifier = new ConsumptionVerifier();
                Func<Task> maybeHangAsync = () => Task.CompletedTask;
                int nMessagesBeingHandled = 0;

                bool trackConsumedMessages = true;

                var consumer = await this.Topic.GetConsumerAsync("myConsumer");
                var parallafka = new Parallafka<string, string>(consumer, parallafkaConfig);
                {
                    var consumedMessages = new ConcurrentBag<IKafkaMessage<string, string>>();
                    var rngs = new ThreadSafeRandom();
                    CancellationTokenSource stopConsuming = new();
                    Task consumeTask = parallafka.ConsumeAsync(async msg =>
                    {
                        // this.Console.WriteLine($"Handler: {msg.Offset}");

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
                    }, stopConsuming.Token);

                    await Wait.UntilAsync("Consumed a bunch of messages", () => Task.FromResult(consumedMessages.Count > 50),
                        timeout: TimeSpan.FromSeconds(45));

                    var hangHandlerTcs = new TaskCompletionSource();
                    maybeHangAsync = () => hangHandlerTcs.Task;
                    // For thread safety, hang all handlers before commencing the shutdown
                    await Wait.UntilAsync("All handlers are hanging",
                        () => Task.FromResult(nMessagesBeingHandled == parallafkaConfig.MaxDegreeOfParallelism), 
                        timeout: TimeSpan.FromSeconds(9));
                    
                    // Stop tracking consumed messages before initiating shutdown
                    // so we know the set that should definitely be committed.
                    trackConsumedMessages = false;

                    hangHandlerTcs.SetResult();

                    stopConsuming.Cancel();

                    await Wait.UntilAsync("Parallafka instance is stopped",
                        () =>
                        {
                            Assert.True(consumeTask.IsCompletedSuccessfully);
                            return Task.CompletedTask;
                        },
                        timeout: TimeSpan.FromSeconds(60));

                    Assert.Equal(0, nMessagesBeingHandled);

                    verifier.AssertAllConsumedMessagesWereCommitted(consumer);
                    verifier.AddSentMessages(consumedMessages);
                    verifier.AssertConsumedAllSentMessagesProperly();

                    await Wait.ForTaskOrTimeoutAsync(consumeTask, TimeSpan.FromSeconds(15),
                        onTimeout: () => throw new Exception("Timed out waiting for consumeTask"));
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