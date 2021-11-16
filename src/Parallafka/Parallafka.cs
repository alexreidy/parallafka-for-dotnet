using System;
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

        public async Task ConsumeAsync(
            Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync,
            CancellationToken stopToken)
        {
            // Are there any deadlocks or performance issues with these caps in general?
            var localStop = new CancellationTokenSource();
            var commitState = new CommitState<TKey, TValue>();
            var messagesByKey = new MessagesByKey<TKey, TValue>();

            // the message router ensures messages are handled by key in order
            var router = new MessageRouter<TKey, TValue>(commitState, messagesByKey, localStop.Token);
            var routingTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(router.RouteMessage,
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 999,
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
                    MaxDegreeOfParallelism = this._config.MaxDegreeOfParallelism
                });

            // This is where messages go after being handled: enqueued to be committed when it's safe.
            var committer = new MessageCommitter<TKey, TValue>(
                this._consumer, 
                commitState, 
                this._logger, 
                this._config.CommitDelay ?? TimeSpan.FromSeconds(5), 
                localStop.Token);
            var committerTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(committer.TryCommitMessage,
                new ExecutionDataflowBlockOptions
                {
                    BoundedCapacity = 333,
                    MaxDegreeOfParallelism = 1
                });


            router.MessagesToHandle.LinkTo(handlerTarget);
            finishedRouter.MessagesToHandle.LinkTo(handlerTarget);

            async Task SendCommitter(IKafkaMessage<TKey, TValue> m)
            {
                if (!await committerTarget.SendAsync(m))
                {
                    WriteLine($"CT: {m.Key} {m.Offset} SendAsync failed!");
                }
            }
            var messageHandledTarget = new ActionBlock<IKafkaMessage<TKey, TValue>>(
                m => Task.WhenAll(
                    finishedRouter.MessageHandlerFinished(m),
                    SendCommitter(m)));

            handler.MessageHandled.LinkTo(messageHandledTarget);
            
            await this.KafkaPollerThread(routingTarget, stopToken);

            routingTarget.Complete();
            await routingTarget.Completion;

            router.MessagesToHandle.Complete();
            await router.MessagesToHandle.Completion;
            
            messagesByKey.Complete();
            await messagesByKey.Completion;

            finishedRouter.MessagesToHandle.Complete();
            await finishedRouter.MessagesToHandle.Completion;

            handlerTarget.Complete();
            await handlerTarget.Completion;

            handler.MessageHandled.Complete();
            await handler.MessageHandled.Completion;
            
            messageHandledTarget.Complete();
            await messageHandledTarget.Completion;
            
            committerTarget.Complete();
            await committerTarget.Completion;

            committer.Complete();
            await committer.Completion;

            WriteLine($"ConsumeFinished: {commitState.GetStats()}");
        }
        
        private async Task KafkaPollerThread(ITargetBlock<IKafkaMessage<TKey, TValue>> routingTarget, CancellationToken stopToken)
        {
            try
            {
                while (!stopToken.IsCancellationRequested)
                {
                    // TODO: Error handling
                    try
                    {
                        IKafkaMessage<TKey, TValue> message = await this._consumer.PollAsync(stopToken);
                        if (message == null)
                        {
                            if (!stopToken.IsCancellationRequested)
                            {
                                this._logger.LogWarning(
                                    "Polled a null message while not shutting down: breach of IKafkaConsumer contract");
                                await Task.Delay(50);
                            }
                        }
                        else
                        {
                            try
                            {
                                // WriteLine($"RT: Sending {message.Key} {message.Offset}");
                                await routingTarget.SendAsync(message, stopToken);
                            }
                            catch (OperationCanceledException)
                            {
                                break;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        this._logger.LogError(e, "Error in Kafka poller thread");
                        await Task.Delay(333);
                    }
                }
            }
            catch (Exception e)
            {
                this._logger.LogCritical(e, "Fatal error in Kafka poller thread");
            }
        }


    }
}