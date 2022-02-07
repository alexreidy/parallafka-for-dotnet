using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    internal class MessageFinishedRouter<TKey, TValue>
    {
        private readonly MessagesByKey<TKey, TValue> _messageByKey;
        private readonly BufferBlock<KafkaMessageWrapped<TKey, TValue>> _messagesToHandle;

        private long _messagesHandled;
        private long _messagesSent;
        private long _messagesNotSent;

        public MessageFinishedRouter(MessagesByKey<TKey, TValue> messageByKey)
        {
            this._messageByKey = messageByKey;
            this._messagesToHandle = new(new DataflowBlockOptions
            {
                BoundedCapacity = 1
            });
        }

        public ISourceBlock<KafkaMessageWrapped<TKey, TValue>> MessagesToHandle => this._messagesToHandle;

        public Task Completion => this._messageByKey.Completion;

        public object GetStats()
        {
            return new
            {
                InputCount = this._messagesToHandle.Count,
                MessagesHandled = this._messagesHandled,
                MessagesSent = this._messagesSent,
                MessagesNotSent = this._messagesNotSent
            };
        }

        public void Complete()
        {
            this._messageByKey.Complete();
        }

        public async Task MessageHandlerFinished(KafkaMessageWrapped<TKey, TValue> message)
        {
            Interlocked.Increment(ref this._messagesHandled);
            // If there are any messages with the same key queued, make the next one available for handling.
            // TODO: Is this safe as far as commits?
            if (this._messageByKey.TryGetNextMessageToHandle(message, out var newMessage))
            {
                if (await this._messagesToHandle.SendAsync(newMessage))
                {
                    Interlocked.Increment(ref this._messagesSent);
                }
                else
                {
                    Interlocked.Increment(ref this._messagesNotSent);
                    Parallafka<TKey, TValue>.WriteLine($"MFR: {newMessage.Key} {newMessage.Offset} SendAsync failed!");
                }
            }
        }

    }
}
