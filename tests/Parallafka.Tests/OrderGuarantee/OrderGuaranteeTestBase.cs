using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.OrderGuarantee
{
    public abstract class OrderGuaranteeTestBase : KafkaTopicTestBase
    {
        public virtual async Task TestFifoOrderIsPreservedForSameKeyAsync()
        {
            KafkaConsumerSpy<string, string> consumer = await this.Topic.GetConsumerAsync($"SameKeyOrderTest-{Guid.NewGuid()}");

            IParallafka<string, string> parallafka = new Parallafka<string, string>(consumer,
                new ParallafkaConfig<string, string>
                {
                    MaxDegreeOfParallelism = 7
                });

            var keys = new List<string>();
            string currentKey = Guid.NewGuid().ToString();
            keys.Add(currentKey);

            var rng = new Random();
            var messagesToSend = new List<IKafkaMessage<string, string>>();
            int totalMessagesSent = 0;
            for (; totalMessagesSent < 3000; totalMessagesSent++)
            {
                if (rng.NextDouble() < 0.07)
                {
                    currentKey = Guid.NewGuid().ToString();
                    keys.Add(currentKey);
                }
                else if (rng.NextDouble() < 0.15)
                {
                    currentKey = keys[rng.Next(keys.Count)];
                }

                string key = currentKey;

                // Add some noise within the burst of currentKey
                if (rng.NextDouble() < 0.2)
                {
                    key = Guid.NewGuid().ToString();
                } else if (rng.NextDouble() < 0.2 && keys.Count > 1)
                {
                    key = keys[keys.Count - 2];
                }
                messagesToSend.Add(new KafkaMessage<string, string>(
                    key: key,
                    value: totalMessagesSent.ToString()));
            }

            Task publishTask = this.Topic.PublishAsync(messagesToSend);

            var consumptionVerifier = new ConsumptionVerifier();
            consumptionVerifier.AddSentMessages(messagesToSend);

            int totalReceived = 0;
            CancellationTokenSource stopConsuming = new CancellationTokenSource();
            await parallafka.ConsumeAsync(async msg =>
            {
                var rng = new Random();
                await Task.Delay(rng.Next(55 + rng.Next(40))); // TODO: remember to apply this in other tests. And use threadsaferandom

                consumptionVerifier.AddConsumedMessages(new[] { msg });

                if (Interlocked.Increment(ref totalReceived) == totalMessagesSent)
                {
                    stopConsuming.Cancel();
                }
            }, stopConsuming.Token);

            await publishTask;
            Assert.Equal(totalMessagesSent, totalReceived);

            consumptionVerifier.AssertConsumedAllSentMessagesProperly();
            consumptionVerifier.AssertAllConsumedMessagesWereCommitted(consumer); // how can we make something that knows when a message has been committed before handler finished?
        }

        protected OrderGuaranteeTestBase(ITestOutputHelper console) : base(console)
        {
        }
    }
}