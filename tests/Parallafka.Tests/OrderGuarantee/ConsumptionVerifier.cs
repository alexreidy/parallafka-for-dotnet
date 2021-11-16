using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests.OrderGuarantee
{
    public class ConsumptionVerifier
    {
        private readonly List<IKafkaMessage<string, string>> _sentMessages = new();

        private readonly HashSet<string> _sentMessageUniqueIds = new();

        private readonly Dictionary<string, Queue<IKafkaMessage<string, string>>> _consumedMessagesByKey = new();

        private int _consumedMessageCount = 0;

        private static string UniqueIdFor(IKafkaMessage<string, string> message)
        {
            // TODO: Override equals
            return $"{message.Key}-[ :) ]-{message.Value}";
        }

        public void AddSentMessages(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            lock (_sentMessages)
            {
                foreach (var message in messages)
                {
                    string id = UniqueIdFor(message);
                    if (!this._sentMessageUniqueIds.Add(id))
                    {
                        throw new Exception($"Sent message already added: {id}");
                    }

                    this._sentMessages.Add(message);
                }
            }
        }

        /// <summary>
        /// Call this when messages are received by a consumer. Thread-safe.
        /// </summary>
        public void AddConsumedMessages(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            foreach (var msg in messages)
            {
                Interlocked.Increment(ref this._consumedMessageCount);

                lock (this._consumedMessagesByKey)
                {
                    if (!this._consumedMessagesByKey.TryGetValue(msg.Key, out var queueForKey))
                    {
                        queueForKey = new();
                        this._consumedMessagesByKey[msg.Key] = queueForKey;
                    }

                    queueForKey.Enqueue(msg);
                }
            }
        }

        public void AddConsumedMessage(IKafkaMessage<string, string> message)
        {
            this.AddConsumedMessages(new[] { message });
        }

        public void AssertConsumedAllSentMessagesProperly()
        {
            Assert.Equal(this._sentMessages.Count, this._consumedMessageCount);

            foreach (var kvp in this._consumedMessagesByKey)
            {
                long prevMsgOffset = -1;
                foreach (IKafkaMessage<string, string> message in kvp.Value)
                {
                    Assert.Equal(kvp.Key, message.Key);
                    
                    long offset = message.Offset.Offset;
                    Assert.True(offset > prevMsgOffset, $"{offset} not > previous message offset {prevMsgOffset}"); // TODO: HOW DID THIS FAIL in order guarantee test

                    prevMsgOffset = offset;

                    Assert.True(this._sentMessageUniqueIds.Contains(UniqueIdFor(message)), "Expecting to find message in list of send messages");
                }
            }
        }

        public void AssertAllConsumedMessagesWereCommitted(KafkaConsumerSpy<string, string> consumer)
        {
            var byPartition = this._consumedMessagesByKey.Values
                .SelectMany(q => q)
                .GroupBy(m => m.Offset.Partition);

            foreach (var partition in byPartition)
            {
                var maxOffset = partition.Max(g => g.Offset.Offset);

                if (!consumer.CommittedOffsets.Any(offset =>
                    offset.Partition == partition.Key && offset.Offset >= maxOffset))
                {
                    Assert.False(true, $"Expecting to find committed offset for P:{partition.Key} O:{maxOffset}");
                }
            }
        }
    }
}