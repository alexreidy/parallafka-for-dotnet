using System.Collections.Generic;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;
using Parallafka.KafkaConsumer.Implementations.Mock;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Performance
{
    public class PerformanceTests : ThroughputTestBase
    {
        private ITestOutputHelper _outputHelper;

        private readonly Dictionary<string, MockConsumer<string, string>> _consumersByGroupId = new();

        private readonly List<IKafkaMessage<string, string>> _topic = new();

        public PerformanceTests(ITestOutputHelper outputHelper) : base(outputHelper)
        {
            this._outputHelper = outputHelper;
        }

        protected override IKafkaConsumer<string, string> GetTestTopicConsumer(string groupId)
        {
            var consumer = new MockConsumer<string, string>();
            consumer.AddRecords(this._topic);
            this._consumersByGroupId[groupId] = consumer;
            return consumer;
        }

        protected override Task PublishToTestTopicAsync(IEnumerable<IKafkaMessage<string, string>> messages)
        {
            // TODO: Make a Kafka mock and rework all this. This expects only one call.
            var msgs = new List<IKafkaMessage<string, string>>();
            int i = 0;
            foreach (var message in messages)
            {
                message.Offset = new RecordOffset(i % 11, i / 11);
                msgs.Add(message);
                i++;
            }
            this._topic.AddRange(msgs);
            foreach (var consumer in this._consumersByGroupId.Values)
            {
                consumer.AddRecords(msgs);
            }
            return Task.CompletedTask;
        }
    }
}