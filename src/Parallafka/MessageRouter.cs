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
                BoundedCapacity = 100
            });
        }

        public ISourceBlock<IKafkaMessage<TKey, TValue>> MessagesToHandle => this._messagesToHandle;

        public async Task RouteMessage(IKafkaMessage<TKey, TValue> message)
        {
            this._commitState.EnqueueMessage(message);

            if (!this._messageByKey.TryAddMessageToHandle(message))
            {
                return;
            }

            if (!await this._messagesToHandle.SendAsync(message, this._stopToken))
            {
                Parallafka<TKey, TValue>.WriteLine($"MR: {message.Key} {message.Offset} SendAsync failed!");
            }
        }
    }
}
