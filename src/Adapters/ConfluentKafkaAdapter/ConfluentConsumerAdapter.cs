using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Parallafka.KafkaConsumer;

namespace Parallafka.Adapters.ConfluentKafka
{
    public class ConfluentConsumerAdapter<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> _confluentConsumer;

        private readonly string _topic;

        public ConfluentConsumerAdapter(IConsumer<TKey, TValue> consumer, string topic)
        {
            this._confluentConsumer = consumer;
            this._topic = topic;
        }

        public Task CommitAsync(IKafkaMessage<TKey, TValue> message)
        {
            // TODO: Does this not accept a CancellationToken? Roll our own?
            this._confluentConsumer.Commit(new[]
            {
                new TopicPartitionOffset(
                    this._topic,
                    message.Offset.Partition,
                    message.Offset.Offset)
            });
            
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            this._confluentConsumer.Dispose();
            return ValueTask.CompletedTask;
        }

        public async Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();
            ConsumeResult<TKey, TValue> result;

            for (;;)
            {
                result = this._confluentConsumer.Consume(cancellationToken);
                if (result.IsPartitionEOF)
                {
                    await Task.Delay(50, cancellationToken);
                }
                else
                {
                    break;
                }
            }

            IKafkaMessage<TKey, TValue> msg = new KafkaMessage<TKey, TValue>(
                result.Message.Key,
                result.Message.Value,
                new RecordOffset(result.Partition, result.Offset));
            return msg;
        }
    }
}