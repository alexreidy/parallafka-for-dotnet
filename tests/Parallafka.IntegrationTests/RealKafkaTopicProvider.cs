using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Parallafka.Adapters.ConfluentKafka;
using Parallafka.KafkaConsumer;
using Parallafka.Tests;

namespace Parallafka.IntegrationTests
{
    /// <summary>
    /// A kafka topic provider using a kafka installation.  Defaults to a local install via docker from docker-compose.kafka.yaml
    /// </summary>
    public class RealKafkaTopicProvider : ITestKafkaTopic
    {
        private readonly IAdminClient _adminClient;

        private readonly string _topicName;

        private readonly ClientConfig _clientConfig;

        private readonly IProducer<string, string> _producer;

        private bool _topicExists = false;

        private readonly SemaphoreSlim _creatorLock = new(1);

        public Task InitializeAsync()
        {
            return this.CreateTopicIfNotExistsAsync();
        }

        public RealKafkaTopicProvider(string topicName = null, ClientConfig clientConfig = null)
        {
            this._topicName = topicName ?? $"test-{Guid.NewGuid()}";
            this._clientConfig = clientConfig ?? new ClientConfig
            {
                BootstrapServers = "localhost:9092",
            };

            var adminClientBuilder = new AdminClientBuilder(this._clientConfig);
            this._adminClient = adminClientBuilder.Build();

            var producerBuilder = new ProducerBuilder<string, string>(this._clientConfig);
            this._producer = producerBuilder.Build();
        }

        public async Task<KafkaConsumerSpy<string, string>> GetConsumerAsync(string groupId)
        {
            await this.CreateTopicIfNotExistsAsync();

            var consumerBuilder = new ConsumerBuilder<string, string>(
                new ConsumerConfig(this._clientConfig)
                {
                    GroupId = groupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false,
                    // EnablePartitionEof = true,
                });
            IConsumer<string, string> consumer = consumerBuilder.Build();
            
            consumer.Subscribe(this._topicName);
            var adapter = new ConfluentConsumerAdapter<string, string>(consumer, this._topicName);
            return new KafkaConsumerSpy<string, string>(adapter);
        }

        public async Task PublishAsync(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            await this.CreateTopicIfNotExistsAsync();
            foreach (var message in messages)
            {
                var msg = new Message<string, string>
                {
                    Key = message.Key,
                    Value = message.Value
                };
                await this._producer.ProduceAsync(this._topicName, msg);
            }
        }

        public Task DeleteAsync()
        {
            if (!this._topicExists)
            {
                return Task.CompletedTask;
            }

            return this._adminClient.DeleteTopicsAsync(new[] { this._topicName });
        }

        private async Task CreateTopicIfNotExistsAsync()
        {
            if (this._topicExists)
            {
                return;
            }

            await _creatorLock.WaitAsync();
            try
            {
                if (this._topicExists)
                {
                    return;
                }

                await this._adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name = this._topicName,
                        NumPartitions = 11,
                    },
                }, new CreateTopicsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(9),
                });

                this._topicExists = true;
            }
            finally
            {
                this._creatorLock.Release();
            }
        }
    }
}