using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    internal class MessageRouter<TKey, TValue>
    {
        private readonly CommitState<TKey, TValue> _commitState;
        private readonly MessagesByKey<TKey, TValue> _messageByKey;
        private readonly CancellationToken _stopToken;
        private readonly BufferBlock<IKafkaMessage<TKey, TValue>> _messagesToHandle;
        private long _messagesRouted;
        private long _messagesSkipped;
        private long _messagesHandled;
        private long _messagesNotHandled;

        public MessageRouter(
            CommitState<TKey, TValue> commitState,
            MessagesByKey<TKey, TValue> messageByKey,
            CancellationToken stopToken)
        {
            this._commitState = commitState;
            this._messageByKey = messageByKey;
            this._stopToken = stopToken;
            this._messagesToHandle = new BufferBlock<IKafkaMessage<TKey, TValue>>(new DataflowBlockOptions
            {
                BoundedCapacity = 1
            });
        }

        public object GetStats()
        {
            return new
            {
                MessagesRouted = this._messagesRouted,
                MessagesHandled = this._messagesHandled,
                MessagesNotHandled = this._messagesNotHandled,
                MessagesSkipped = this._messagesSkipped,
                IncomingQueueSize = this._messagesToHandle.Count
            };
        }

        public ISourceBlock<IKafkaMessage<TKey, TValue>> MessagesToHandle => this._messagesToHandle;

        public async Task RouteMessage(IKafkaMessage<TKey, TValue> message)
        {
            Interlocked.Increment(ref _messagesRouted);

            await this._commitState.EnqueueMessageAsync(message);

            if (!this._messageByKey.TryAddMessageToHandle(message))
            {
                Interlocked.Increment(ref _messagesSkipped);
                return;
            }

            try
            {
                if (!await this._messagesToHandle.SendAsync(message, this._stopToken))
                {
                    Interlocked.Increment(ref _messagesNotHandled);
                    Parallafka<TKey, TValue>.WriteLine($"MR: {message.Key} {message.Offset} SendAsync failed!");
                }
                else
                {
                    Interlocked.Increment(ref _messagesHandled);
                }
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
        }
    }
}
