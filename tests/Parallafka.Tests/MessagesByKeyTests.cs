using System.Linq;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests
{
    public class MessagesByKeyTests
    {
        [Fact]
        public void GetNextMessageWorksCorrectly()
        {
            var mbk = new MessagesByKey<string, string>(default);
            var km1 = KafkaMessage.Create("key1", "value", new RecordOffset(0, 0)).Wrapped();
            var km2 = KafkaMessage.Create("key2", "value", new RecordOffset(0, 1)).Wrapped();
            var km3 = KafkaMessage.Create("key3", "value", new RecordOffset(0, 2)).Wrapped();
            Assert.True(mbk.TryAddMessageToHandle(km1));
            Assert.False(mbk.TryAddMessageToHandle(km1));
            Assert.True(mbk.TryAddMessageToHandle(km2));

            KafkaMessageWrapped<string, string> nextMessage;
            Assert.True(mbk.TryGetNextMessageToHandle(km1, out nextMessage));
            Assert.Equal(km1, nextMessage);
            Assert.False(mbk.TryGetNextMessageToHandle(km1, out nextMessage));
            Assert.Null(nextMessage);
            Assert.False(mbk.TryGetNextMessageToHandle(km2, out nextMessage));
            Assert.Null(nextMessage);
            Assert.False(mbk.TryGetNextMessageToHandle(km3, out nextMessage));
            Assert.Null(nextMessage);
        }

        [Fact]
        public void CompletionMessageWorksCorrectly()
        {
            var mbk = new MessagesByKey<int, int>(default);
            var kms = Enumerable.Range(1, 50).Select(i => KafkaMessage.Create(i % 10, i, new RecordOffset(0, i)).Wrapped()).ToList();

            foreach (var km in kms)
            {
                mbk.TryAddMessageToHandle(km);
            }

            mbk.Complete();
            
            Assert.False(mbk.Completion.IsCompleted);

            foreach (var km in kms.Skip(1))
            {
                mbk.TryGetNextMessageToHandle(km, out _);
                Assert.False(mbk.Completion.IsCompleted);
            }

            mbk.TryGetNextMessageToHandle(kms[0], out _);
            Assert.True(mbk.Completion.IsCompleted);
        }
    }
}
