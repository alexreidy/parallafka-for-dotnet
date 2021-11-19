using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    /// <summary>
    /// Maps the Kafka message key to a queue of as yet unhandled messages with the key, or null rather
    /// than a queue if no further messages with the key have arrived and started piling up.
    /// The presence of the key in this dictionary lets us track which messages are "handling in progress,"
    /// while the queue, if present, holds the next messages with the key to handle, in FIFO order, preserving
    /// Kafka's same-key order guarantee.
    /// </summary>
    internal class MessagesByKey<TKey, TValue>
    {
        private readonly Dictionary<TKey, Queue<IKafkaMessage<TKey, TValue>>> _messagesToHandleForKey;
        private readonly TaskCompletionSource _completedSource;
        private bool _completed;

        public MessagesByKey()
        {
            this._messagesToHandleForKey = new();
            this._completedSource = new();
            this.Completion = this._completedSource.Task;
        }

        /// <summary>
        /// Indicates to the instance that processing should be finished when there's no more work queued
        /// </summary>
        public void Complete()
        {
            this._completed = true;
            lock (this._messagesToHandleForKey)
            {
                if (this._messagesToHandleForKey.Count == 0)
                {
                    this._completedSource.TrySetResult();
                }
            }
        }
        
        /// <summary>
        /// A task that is completed when the instance is finished processing
        /// </summary>
        public Task Completion { get; private set; }
        
        /// <summary>
        /// Given a message, attempts to return another message to handle for the same message key
        /// </summary>
        /// <param name="message">The message that was handled</param>
        /// <param name="nextMessage">The next message that should be handled</param>
        /// <returns>True if a nextMessage was found</returns>
        public bool TryGetNextMessageToHandle(IKafkaMessage<TKey, TValue> message, [NotNullWhen(true)] out IKafkaMessage<TKey, TValue> nextMessage)
        {
            lock (this._messagesToHandleForKey)
            {
                if (!this._messagesToHandleForKey.TryGetValue(message.Key, out var messagesQueuedForKey))
                {
                    // shouldn't happen
                    nextMessage = null;
                    Parallafka<TKey, TValue>.WriteLine($"MBK:GetNext: {message.Key} {message.Offset} [none]");
                    return false;
                }

                if (messagesQueuedForKey == null || messagesQueuedForKey.Count == 0)
                {
                    this._messagesToHandleForKey.Remove(message.Key);

                    if (this._completed && this._messagesToHandleForKey.Count == 0)
                    {
                        this._completedSource.TrySetResult();
                    }

                    nextMessage = null;

                    Parallafka<TKey, TValue>.WriteLine($"MBK:GetNext: {message.Key} {message.Offset} [none]");

                    return false;
                }

                nextMessage = messagesQueuedForKey.Dequeue();

                Parallafka<TKey, TValue>.WriteLine($"MBK:GetNext: {message.Key} {message.Offset} Next:{nextMessage.Offset}");

                return true;
            }
        }

        /// <summary>
        /// Returns true if the message should be handled, false otherwise
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public bool TryAddMessageToHandle(IKafkaMessage<TKey, TValue> message)
        {
            lock (this._messagesToHandleForKey)
            {
                var aMessageWithThisKeyIsCurrentlyBeingHandled = this._messagesToHandleForKey.TryGetValue(message.Key, out var messagesToHandleForKey);

                if (aMessageWithThisKeyIsCurrentlyBeingHandled)
                {
                    if (messagesToHandleForKey == null)
                    {
                        messagesToHandleForKey = new Queue<IKafkaMessage<TKey, TValue>>();
                        this._messagesToHandleForKey[message.Key] = messagesToHandleForKey;
                    }

                    Parallafka<TKey, TValue>.WriteLine($"MBK:Enqueuing: {message.Key} {message.Offset}");

                    messagesToHandleForKey.Enqueue(message);

                    return false;
                }

                // Add the key to indicate that a message with the key is being handled (see above)
                // so we know to queue up any additional messages with the key.
                // Without this line, FIFO same-key handling order is not enforced.
                // Remove it to test the tests.
                lock (this._messagesToHandleForKey)
                {
                    this._messagesToHandleForKey[message.Key] = null;
                }

                Parallafka<TKey, TValue>.WriteLine($"MBK:Processing: {message.Key} {message.Offset}");

                return true;
            }
        }
    }
}
