using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    internal class MessageHandler<TKey, TValue>
    {
        /// <summary>
        /// The function that actually processes the message
        /// </summary>
        private readonly Func<IKafkaMessage<TKey, TValue>, Task> _messageHandlerAsync;
        
        private readonly ILogger _logger;
        private readonly BufferBlock<KafkaMessageWrapped<TKey, TValue>> _messageHandled;
        private readonly CancellationToken _stopToken;

        private long _messagesHandled;
        private long _messagesErrored;
        private long _messagesSent;
        private long _messagesNotSent;

        public MessageHandler(
            Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync,
            ILogger logger,
            CancellationToken stopToken)
        {
            this._messageHandlerAsync = messageHandlerAsync;
            this._logger = logger;
            this._messageHandled = new BufferBlock<KafkaMessageWrapped<TKey, TValue>>(new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 100
            });
            this._stopToken = stopToken;
        }

        /// <summary>
        /// Messages handled by HandleMessage are sent to this source block
        /// </summary>
        public ISourceBlock<KafkaMessageWrapped<TKey, TValue>> MessageHandled => this._messageHandled;

        public object GetStats()
        {
            return new
            {
                InputCount = this._messageHandled.Count,
                MessagesHandled = this._messagesHandled,
                MessagesSent = this._messagesSent,
                MessagesNotSent = this._messagesNotSent,
                MessagesErrored = this._messagesErrored
            };
        }

        public async Task HandleMessage(KafkaMessageWrapped<TKey, TValue> message)
        {
            Interlocked.Increment(ref this._messagesHandled);

            try
            {
                await this._messageHandlerAsync(message.Message);
            }
            catch (Exception e)
            {
                Interlocked.Increment(ref this._messagesErrored);
                // TODO: User is responsible for handling errors but should we do anything else here?
                this._logger.LogError(e,
                    $"Unhandled exception in handler callback. Warning: Still attempting to commit this and handle further messages. Partition={message.Offset.Partition}, Offset={message.Offset.Offset}");
            }

            message.SetIsReadyToCommit();

            Parallafka<TKey, TValue>.WriteLine($"Handler: {message.Key} {message.Offset} WasHandled");

            try
            {
                if (await this._messageHandled.SendAsync(message, this._stopToken))
                {
                    Interlocked.Increment(ref this._messagesSent);
                }
                else
                {
                    Interlocked.Increment(ref this._messagesNotSent);
                    Parallafka<TKey, TValue>.WriteLine($"Handler: {message.Key} {message.Offset} SendAsync failed!");
                }
            }
            catch (OperationCanceledException)
            {
                Interlocked.Increment(ref this._messagesNotSent);
            }
        }
    }
}
