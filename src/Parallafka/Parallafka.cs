using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;
using Microsoft.Extensions.Logging;

namespace Parallafka
{
    public class Parallafka<TKey, TValue> : IParallafka<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;
        private readonly IParallafkaConfig<TKey, TValue> _config;
        private readonly ILogger _logger;
        private Func<object> _getStats;

        public static Action<string> WriteLine { get; set; } = (string s) => { };

        public Parallafka(
            IKafkaConsumer<TKey, TValue> consumer,
            IParallafkaConfig<TKey, TValue> config)
        {
            this._consumer = consumer;
            this._config = config;
            this._logger = config.Logger;

            // TODO: Configurable caps, good defaults.
        }

        public object GetStats()
        {
            var gs = _getStats;
            if (gs == null)
            {
                return new { };
            }

            return gs();
        }

        /// <inheritdoc />
        public async Task ConsumeAsync(
            Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync,
            CancellationToken stopToken)
        {
            var runtime = Stopwatch.StartNew();
            var maxQueuedMessages = this._config.MaxQueuedMessages ?? 1000;
            // Are there any deadlocks or performance issues with these caps in general?
            using var localStop = new CancellationTokenSource();
            var commitState = new CommitState<TKey, TValue>(maxQueuedMessages, localStop.Token);
            var messagesByKey = new MessagesByKey<TKey, TValue>();

            // the message router ensures messages are handled by key in order
            var router = new MessageRouter<TKey, TValue>(commitState, messagesByKey, localStop.Token);
            var routingTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(router.RouteMessage,
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1,
                    MaxDegreeOfParallelism = 1
                });

            var finishedRouter = new MessageFinishedRouter<TKey, TValue>(messagesByKey);

            // Messages eligible for handler threads to pick up and handle.
            var handler = new MessageHandler<TKey, TValue>(
                messageHandlerAsync,
                this._logger,
                localStop.Token);
            var handlerTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(handler.HandleMessage,
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = Math.Max(maxQueuedMessages, this._config.MaxDegreeOfParallelism),
                    MaxDegreeOfParallelism = this._config.MaxDegreeOfParallelism
                });

            // This is where messages go after being handled: enqueued to be committed when it's safe.
            var committer = new MessageCommitter<TKey, TValue>(
                this._consumer, 
                commitState, 
                this._logger);
            var committerTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(async m =>
                {
                    if (this._config.CommitDelay > TimeSpan.Zero)
                    {
                        await Task.Delay(this._config.CommitDelay, localStop.Token);
                    }

                    await committer.CommitNow(localStop.Token);
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 2,
                    MaxDegreeOfParallelism = 1
                });

            router.MessagesToHandle.LinkTo(handlerTarget);
            finishedRouter.MessagesToHandle.LinkTo(handlerTarget);

            // handled messages are sent to both:
            // . the finished router (send the next message for the key)
            // . the committer
            var messageHandledTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(
                m =>
                {
                    // it is OK if the Post returns false, this means there's already a commit processed
                    committerTarget.Post(m);
                    return finishedRouter.MessageHandlerFinished(m);
                },
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 1000
                });
            handler.MessageHandled.LinkTo(messageHandledTarget);

            var state = "Polling for Kafka messages";
            this._getStats = () =>
                new
                {
                    ConsumerState = state,
                    MaxQueuedMessages = maxQueuedMessages,
                    CallDuration = runtime.Elapsed.ToString("g"),
                    CallDurationMs = runtime.ElapsedMilliseconds,
                    CommitState = commitState.GetStats(),
                    MessagesByKey = messagesByKey.GetStats(),
                    Router = router.GetStats(),
                    RoutingTarget = new
                    {
                        routingTarget.InputCount,
                        TaskStatus = routingTarget.Completion.Status.ToString()
                    },
                    FinishedRouter = finishedRouter.GetStats(),
                    Handler = handler.GetStats(),
                    HandlerTarget = new
                    {
                        handlerTarget.InputCount,
                        TaskStatus = handlerTarget.Completion.Status.ToString()
                    },
                    Committer = committer.GetStats(),
                    CommitterTarget = new
                    {
                        committerTarget.InputCount,
                        TaskStatus = committerTarget.Completion.Status.ToString()
                    },
                    MessageHandledTarget = new
                    {
                        messageHandledTarget.InputCount,
                        TaskStatus = messageHandledTarget.Completion.Status.ToString()
                    }
                };

            // poll kafka for messages and send them to the routingTarget
            await this.KafkaPollerThread(routingTarget, stopToken);

            // done polling, wait for the routingTarget to finish
            state = "Shutdown: Awaiting message routing";
            routingTarget.Complete();
            await routingTarget.Completion;

            // wait for the router to finish (it should already be done)
            state = "Shutdown: Awaiting message handler";
            router.MessagesToHandle.Complete();
            await router.MessagesToHandle.Completion;

            // wait for the finishedRoute to complete handling all the queued messages
            finishedRouter.Complete();
            state = "Shutdown: Awaiting message routing completion";
            await finishedRouter.Completion;

            // wait for the message handler to complete (should already be done)
            state = "Shutdown: Awaiting handler shutdown";
            handlerTarget.Complete();
            await handlerTarget.Completion;

            state = "Shutdown: Awaiting handled shutdown";
            handler.MessageHandled.Complete();
            await handler.MessageHandled.Completion;

            state = "Shutdown: Awaiting handled target shutdown";
            messageHandledTarget.Complete();
            await messageHandledTarget.Completion;

            // wait for the committer to finish
            state = "Shutdown: Awaiting message committer target";
            committerTarget.Complete();
            await committerTarget.Completion;

            this._getStats = null;

            // commitState should be empty
            WriteLine("ConsumeFinished");
        }
        
        private async Task KafkaPollerThread(ITargetBlock<IKafkaMessage<TKey, TValue>> routingTarget, CancellationToken stopToken)
        {
            int? delay = null;

            try
            {
                for(;;)
                {
                    // TODO: Error handling
                    try
                    {
                        if (delay.HasValue)
                        {
                            await Task.Delay(delay.Value, stopToken);
                            delay = null;
                        }

                        IKafkaMessage<TKey, TValue> message = await this._consumer.PollAsync(stopToken);
                        if (message == null)
                        {
                            stopToken.ThrowIfCancellationRequested();
                            this._logger.LogWarning("Polled a null message while not shutting down: breach of IKafkaConsumer contract");
                            delay = 50;
                        }
                        else
                        {
                            WriteLine($"Poller: Sending {message.Key} {message.Offset}");
                            await routingTarget.SendAsync(message, stopToken);
                            WriteLine($"Poller: Sent {message.Key} {message.Offset}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        this._logger.LogError(e, "Error in Kafka poller thread");
                        delay = 333;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                this._logger.LogCritical(e, "Fatal error in Kafka poller thread");
            }
        }
    }
}
