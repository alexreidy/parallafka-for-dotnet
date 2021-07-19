using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests.OrderGuarantee
{
    public abstract class OrderGuaranteeTestBase : KafkaTopicTestBase
    {
        [Fact]
        public virtual async Task TestFifoOrderIsPreservedForSameKeyAsync()
        {
            IKafkaConsumer<string, string> consumer = await this.Topic.GetConsumerAsync(
                $"SameKeyOrderTest-{Guid.NewGuid().ToString()}");

            IParallafka<string, string> parallafka = new Parallafka<string, string>(consumer,
                new ParallafkaConfig()
                {
                    MaxConcurrentHandlers = 7
                });

            var keys = new List<string>();
            string currentKey = Guid.NewGuid().ToString();
            keys.Add(currentKey);

            var rng = new Random();
            var messagesToSend = new List<IKafkaMessage<string, string>>();
            int totalMessagesSent = 0;
            for (; totalMessagesSent < 3000; totalMessagesSent++)
            {
                if (rng.NextDouble() < 0.1) // TODO: Make this better and sure to test what we want
                {
                    currentKey = Guid.NewGuid().ToString();
                    keys.Add(currentKey);
                }
                else if (rng.NextDouble() < 0.15)
                {
                    currentKey = keys[rng.Next(keys.Count)];
                }
                messagesToSend.Add(new KafkaMessage<string, string>(
                    key: currentKey,
                    value: totalMessagesSent.ToString()));
            }

            Task publishTask = this.Topic.PublishAsync(messagesToSend);

            var consumptionVerifier = new ConsumptionVerifier();
            consumptionVerifier.AddSentMessages(messagesToSend);

            int totalReceived = 0;
            await parallafka.ConsumeAsync(async msg =>
            {
                var rng = new Random();
                await Task.Delay(rng.Next(55 + rng.Next(40)));

                consumptionVerifier.AddConsumedMessages(new[] { msg });

                if (Interlocked.Increment(ref totalReceived) == totalMessagesSent)
                {
                    await parallafka.DisposeAsync();
                }
            });

            await publishTask;
            Assert.Equal(totalMessagesSent, totalReceived);

            consumptionVerifier.AssertConsumedAllSentMessagesProperly();
        }
    }
}