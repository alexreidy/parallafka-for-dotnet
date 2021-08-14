using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;

#pragma warning disable CS4014

namespace Parallafka.Tests.Contracts
{
    public abstract class ConsumerPollTestsBase : KafkaTopicTestBase
    {
        private volatile bool _consumerShouldNotBeHandlingAnyMessages = false;

        [Fact]
        public virtual async Task ConsumerHangsAtPartitionEndsTillNewMessageAsync()
        {
            await using(IKafkaConsumer<string, string> consumer = await this.Topic.GetConsumerAsync("parallafka"))
            await using(var parallafka = new Parallafka<string, string>(consumer,
                new ParallafkaConfig<string, string>()
                {
                    MaxConcurrentHandlers = 7,
                }))
            {
                await this.AssertConsumerHangsAtPartitionEndsTillNewMessageAsync(
                    consumeTopicAsync: parallafka.ConsumeAsync,
                    stopConsumerAsync: () => parallafka.DisposeAsync().AsTask());
            }
        }

        [Fact]
        public virtual async Task RawConsumerHangsAtPartitionEndsTillNewMessageOrCancellationAsync()
        {
            bool receivedNullMsg = false;
            var cts = new CancellationTokenSource(60_000);
            await using(IKafkaConsumer<string, string> consumer = await this.Topic.GetConsumerAsync("rawConsumer"))
            {
                await this.AssertConsumerHangsAtPartitionEndsTillNewMessageAsync(consumeTopicAsync: async handleAsync =>
                {
                    while (true)
                    {
                        IKafkaMessage<string, string> message = await consumer.PollAsync(cts.Token);
                        if (message == null)
                        {
                            receivedNullMsg = true;
                            break;
                        }
                        await handleAsync(message);
                    }
                }, stopConsumerAsync: async () =>
                {
                    Assert.False(receivedNullMsg);
                    cts.Cancel();
                });
                
                Assert.True(receivedNullMsg);
            }
        }

        private async Task AssertConsumerHangsAtPartitionEndsTillNewMessageAsync(
            Func<Func<IKafkaMessage<string, string>, Task>, Task> consumeTopicAsync,
            Func<Task> stopConsumerAsync)
        {
            int nFirstBatchMessagesPublished = 50;
            int nSecondBatchMessagesPublished = 30;
            await this.PublishTestMessagesAsync(nFirstBatchMessagesPublished);

            var messagesHandled = new ConcurrentQueue<IKafkaMessage<string, string>>();

            // Allows other threads to pass actions (e.g. assertions, exception throws) to the main test thread.
            var actionsToRunOnMainThread = new ConcurrentQueue<Action>();
            void RunAssertionsFromOtherThreads()
            {
                foreach (var action in actionsToRunOnMainThread)
                {
                    action.Invoke();
                }
            }

            var consumerLock = new SemaphoreSlim(1);
            async Task WithConsumerLockAsync(Func<Task> actAsync)
            {
                consumerLock.Wait();
                try
                {
                    await actAsync.Invoke();
                }
                finally
                {
                    consumerLock.Release();
                }
            }

            Task consumerTask = consumeTopicAsync(async msg =>
            {
                await WithConsumerLockAsync(async () =>
                {
                    if (messagesHandled.Any(m => m.Key == msg.Key && m.Value == msg.Value))
                    {
                        actionsToRunOnMainThread.Enqueue(() => throw new Exception(
                            $"Already handled this message. Key={msg?.Key}, Value={msg?.Value}"));
                    }

                    if (this._consumerShouldNotBeHandlingAnyMessages)
                    {
                        actionsToRunOnMainThread.Enqueue(() => throw new Exception(
                            $"Consumer received message when it should not have: Key={msg?.Key}, Value={msg?.Value}. Total messages handled: {messagesHandled.Count}"));
                    }

                    messagesHandled.Enqueue(msg);

                    if (messagesHandled.Count == nFirstBatchMessagesPublished ||
                        messagesHandled.Count == nFirstBatchMessagesPublished + nSecondBatchMessagesPublished)
                    {
                        this._consumerShouldNotBeHandlingAnyMessages = true;
                    }
                });
            });

            Task WaitForAllMessagesAndAssertNothingElseIsHandledAfterDelayAsync(int expectedHandledMsgCount)
            {
                return RetryUntilAsync(async () =>
                {
                    RunAssertionsFromOtherThreads();

                    if (this._consumerShouldNotBeHandlingAnyMessages)
                    {
                        // Wait and make sure nothing unexpected and contract-breaching
                        // comes through after all records have been consumed.
                        await Task.Delay(5000);
                    }

                    RunAssertionsFromOtherThreads();
                    Assert.Equal(expectedHandledMsgCount, messagesHandled.Count);
                    Assert.DoesNotContain(null, messagesHandled);
                },
                retryDelay: TimeSpan.FromMilliseconds(80),
                timeout: TimeSpan.FromSeconds(45));
            }

            await WaitForAllMessagesAndAssertNothingElseIsHandledAfterDelayAsync(
                expectedHandledMsgCount: nFirstBatchMessagesPublished);

            this._consumerShouldNotBeHandlingAnyMessages = false;
            await this.PublishTestMessagesAsync(nSecondBatchMessagesPublished, startNum: nFirstBatchMessagesPublished + 1);
            await WaitForAllMessagesAndAssertNothingElseIsHandledAfterDelayAsync(
                expectedHandledMsgCount: nFirstBatchMessagesPublished + nSecondBatchMessagesPublished);

            await stopConsumerAsync();
            await consumerTask;
        }

        private async Task RetryUntilAsync(Func<Task> assertionAsync, TimeSpan retryDelay, TimeSpan timeout)
        {
            var timeoutTask = Task.Delay(timeout);
            while (true)
            {
                try
                {
                    await assertionAsync.Invoke();
                    return;
                }
                catch (Exception)
                {
                }

                if (timeoutTask.IsCompleted)                
                {
                    await assertionAsync.Invoke();
                }

                await Task.Delay(retryDelay);
            }
        }
    }
}