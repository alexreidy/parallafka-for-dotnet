using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Parallafka.Adapters.ConfluentKafka;
using Parallafka.KafkaConsumer;
using Parallafka.Tests.Performance;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.AdapterTests.ConfluentKafka
{
    public class ThroughputTests : ThroughputTestBase, IAsyncLifetime
    {
        private readonly ClientConfig _clientConfig;

        private readonly string _topicName;
        
        private readonly IProducer<string, string> _producer;

        private readonly IAdminClient _adminClient;

        public ThroughputTests(ITestOutputHelper output) : base(output)
        {
            this._clientConfig = new ClientConfig()
            {
                BootstrapServers = "localhost:9092",
            };

            this._topicName = $"ParallafkaThroughputTest-{Guid.NewGuid().ToString()}";

            var adminClientBuilder = new AdminClientBuilder(this._clientConfig);
            this._adminClient = adminClientBuilder.Build();

            var producerBuilder = new ProducerBuilder<string, string>(this._clientConfig);
            this._producer = producerBuilder.Build();
        }

        public async Task InitializeAsync()
        {
            try
            {
                await this._adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = this._topicName,
                        NumPartitions = 11,
                    },
                }, new CreateTopicsOptions()
                {
                    RequestTimeout = TimeSpan.FromSeconds(9),
                });
            }
            catch (CreateTopicsException e) when (e.Message.Contains("already exists"))
            {
            }
        }

        public Task DisposeAsync()
        {
            return this._adminClient.DeleteTopicsAsync(new[] { this._topicName });
        }

        protected override IKafkaConsumer<string, string> GetTestTopicConsumer(string groupId)
        {
            var consumerBuilder = new ConsumerBuilder<string, string>(new ConsumerConfig(this._clientConfig)
            {
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            });
            IConsumer<string, string> consumer = consumerBuilder.Build();
            consumer.Subscribe(this._topicName);
            return new ConfluentConsumerAdapter<string, string>(consumer, this._topicName);
        }

        protected override async Task PublishToTestTopicAsync(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            foreach (var message in messages)
            {
                var msg = new Message<string, string>();
                msg.Key = message.Key;
                msg.Value = message.Value;
                await this._producer.ProduceAsync(this._topicName, msg);
            }
        }
    }
}