using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Parallafka.Adapters.ConfluentKafka;
using Parallafka.KafkaConsumer;
using Parallafka.Tests;

namespace Parallafka.IntegrationTests
{
    public class TestKafkaTopicProvider : ITestKafkaTopic
    {
        private IAdminClient _adminClient;

        private string _topicName;

        private ClientConfig _clientConfig;

        private IProducer<string, string> _producer;

        private bool _topicExists = false;

        public TestKafkaTopicProvider(string topicName, ClientConfig clientConfig = null)
        {
            this._topicName = topicName;
            this._clientConfig = clientConfig ?? new ClientConfig()
            {
                BootstrapServers = "localhost:9092",
            };

            var adminClientBuilder = new AdminClientBuilder(this._clientConfig);
            this._adminClient = adminClientBuilder.Build();

            var producerBuilder = new ProducerBuilder<string, string>(this._clientConfig);
            this._producer = producerBuilder.Build();
        }

        public async Task<IKafkaConsumer<string, string>> GetConsumerAsync(string groupId)
        {
            await this.CreateTopicIfNotExistsAsync();

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

        public async Task PublishAsync(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            await this.CreateTopicIfNotExistsAsync();
            foreach (var message in messages)
            {
                var msg = new Message<string, string>();
                msg.Key = message.Key;
                msg.Value = message.Value;
                await this._producer.ProduceAsync(this._topicName, msg);
            }
        }

        public Task DeleteAsync()
        {
            return this._adminClient.DeleteTopicsAsync(new[] { this._topicName });
        }

        private async Task CreateTopicIfNotExistsAsync()
        {
            if (this._topicExists)
            {
                return;
            }

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

                this._topicExists = true;
            }
            catch (Exception)
            {
            }
        }
    }
}