using System;
using System.Collections.Generic;
using System.Linq;
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

        public Task CommitAsync(IEnumerable<IRecordOffset> offsets)
        {
            this._confluentConsumer.Commit(
                offsets.Select(o => new TopicPartitionOffset(
                    this._topic,
                    o.Partition,
                    o.Offset)));
            
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            this._confluentConsumer.Dispose();
            return ValueTask.CompletedTask;
        }

        public Task<IKafkaMessage<TKey, TValue>> PollAsync(CancellationToken cancellationToken)
        {
            ConsumeResult<TKey, TValue> result;
            try
            {
                result = this._confluentConsumer.Consume(cancellationToken);
            }
            catch (OperationCanceledException e)
            {
                return Task.FromResult((IKafkaMessage<TKey, TValue>)null);
            }
            if (result.IsPartitionEOF)
            {
                return Task.FromResult((IKafkaMessage<TKey, TValue>)null);
            }
            IKafkaMessage<TKey, TValue> msg = new KafkaMessage<TKey, TValue>(result.Message.Key, result.Message.Value,
                new RecordOffset(result.Partition, result.Offset));
            return Task.FromResult(msg);
        }
    }
}