using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;

namespace Parallafka.Tests.Helpers
{
    public class FakeTestKafkaTopic : ITestKafkaTopic
    {
        private readonly FakeConsumer _myConsumer = new();
        private readonly long[] _partitionOffsets;

        public FakeTestKafkaTopic(int numPartitions = 3)
        {
            this._partitionOffsets = new long[numPartitions];
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public Task<KafkaConsumerSpy<string, string>> GetConsumerAsync(string groupId)
        {
            return Task.FromResult(new KafkaConsumerSpy<string, string>(this._myConsumer));
        }

        public async Task PublishAsync(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            foreach (var message in messages)
            {
                var partition = ComputePartition(message.Key);
                var offset = Interlocked.Increment(ref this._partitionOffsets[partition]) - 1;

                var newMessage = KafkaMessage.Create(
                    key: message.Key,
                    value: message.Value,
                    offset: new RecordOffset(partition, offset));

                await this._myConsumer.Messages.SendAsync(newMessage);
            }
        }

        private int ComputePartition(string key)
        {
            return (int)((uint)key.GetHashCode() % this._partitionOffsets.Length);
        }

        private class FakeConsumer : IKafkaConsumer<string, string>
        {
            private readonly BufferBlock<IKafkaMessage<string, string>> _messages = new();

            public ITargetBlock<IKafkaMessage<string, string>> Messages => this._messages;

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }

            public Task<IKafkaMessage<string, string>> PollAsync(CancellationToken cancellationToken)
            {
                return this._messages.ReceiveAsync(cancellationToken);
            }

            public Task CommitAsync(IKafkaMessage<string, string> message)
            {
                return Task.CompletedTask;
            }
        }
    }
}
