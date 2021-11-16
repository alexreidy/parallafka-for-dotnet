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
        private readonly BufferBlock<IKafkaMessage<TKey, TValue>> _messageHandled;
        private readonly CancellationToken _stopToken;

        public MessageHandler(
            Func<IKafkaMessage<TKey, TValue>, Task> messageHandlerAsync,
            ILogger logger,
            CancellationToken stopToken)
        {
            this._messageHandlerAsync = messageHandlerAsync;
            this._logger = logger;
            this._messageHandled = new BufferBlock<IKafkaMessage<TKey, TValue>>(); 
            this._stopToken = stopToken;
        }

        /// <summary>
        /// Messages handled by HandleMessage are sent to this source block
        /// </summary>
        public ISourceBlock<IKafkaMessage<TKey, TValue>> MessageHandled => this._messageHandled;

        public async Task HandleMessage(IKafkaMessage<TKey, TValue> message)
        {
            try
            {
                await this._messageHandlerAsync(message);
            }
            catch (Exception e)
            {
                // TODO: User is responsible for handling errors but should we do anything else here?
                this._logger.LogError(e,
                    $"Unhandled exception in handler callback. Warning: Still attempting to commit this and handle further messages. Partition={message.Offset.Partition}, Offset={message.Offset.Offset}");
            }

            message.WasHandled = true;

            Parallafka<TKey, TValue>.WriteLine($"Handler: {message.Key} {message.Offset} WasHandled");

            try
            {
                if (!await this._messageHandled.SendAsync(message, this._stopToken))
                {
                    Parallafka<TKey, TValue>.WriteLine($"Handler: {message.Key} {message.Offset} SendAsync failed!");
                }
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}
