using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    internal class MessageFinishedRouter<TKey, TValue>
    {
        private readonly MessagesByKey<TKey, TValue> _messageByKey;
        private readonly BufferBlock<IKafkaMessage<TKey, TValue>> _messagesToHandle;

        public MessageFinishedRouter(MessagesByKey<TKey, TValue> messageByKey)
        {
            this._messageByKey = messageByKey;
            this._messagesToHandle = new(new DataflowBlockOptions
            {
                BoundedCapacity = 1
            });
        }

        public ISourceBlock<IKafkaMessage<TKey, TValue>> MessagesToHandle => this._messagesToHandle;

        public Task Completion => this._messageByKey.Completion;

        public void Complete()
        {
            this._messageByKey.Complete();
        }

        public async Task MessageHandlerFinished(IKafkaMessage<TKey, TValue> message)
        {
            // If there are any messages with the same key queued, make the next one available for handling.
            // TODO: Is this safe as far as commits?
            if (this._messageByKey.TryGetNextMessageToHandle(message, out var newMessage))
            {
                if (!await this._messagesToHandle.SendAsync(newMessage))
                {
                    Parallafka<TKey, TValue>.WriteLine($"MFR: {newMessage.Key} {newMessage.Offset} SendAsync failed!");
                }
            }
        }

    }
}
