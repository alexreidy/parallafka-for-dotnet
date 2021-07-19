using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests.OrderGuarantee
{
    public class ConsumptionVerifier
    {
        private List<IKafkaMessage<string, string>> _sentMessages = new();

        private HashSet<string> _sentMessageUniqueIds = new();

        private ConcurrentDictionary<string, ConcurrentQueue<IKafkaMessage<string, string>>> _consumedMessagesByKey = new();

        private int _consumedMessageCount = 0;

        private string UniqueIdFor(IKafkaMessage<string, string> message)
        {
            return $"{message.Key}-[ :) ]-{message.Value}";
        }

        public void AddSentMessages(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            lock (_sentMessages)
            {
                foreach (var message in messages)
                {
                    string id = this.UniqueIdFor(message);
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
                this._consumedMessagesByKey.AddOrUpdate(msg.Key,
                    addValueFactory: k =>
                    {
                        var queueForKey = new ConcurrentQueue<IKafkaMessage<string, string>>();
                        queueForKey.Enqueue(msg);
                        return queueForKey;
                    },
                    updateValueFactory: (k, queueForKey) =>
                    {
                        queueForKey.Enqueue(msg);
                        return queueForKey;
                    });
            }
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
                    
                    //int msgNum = int.Parse(message.Value);
                    long offset = message.Offset.Offset;
                    Assert.True(offset > prevMsgOffset);

                    prevMsgOffset = offset;

                    Assert.True(this._sentMessageUniqueIds.Contains(UniqueIdFor(message)));
                }
            }
        }

    }
}