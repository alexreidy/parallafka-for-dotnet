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

        private ConcurrentDictionary<string, ConcurrentQueue<IKafkaMessage<string, string>>> _consumedMessagesByKey = new();

        private int _consumedMessageCount = 0;

        public void AddSentMessages(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            lock (_sentMessages)
            {
                this._sentMessages.AddRange(messages);
            }
        }

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
                }
            }
        }
    }
}