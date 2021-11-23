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
            var mbk = new MessagesByKey<string, string>();
            var km1 = new KafkaMessage<string, string>("key1", "value");
            var km2 = new KafkaMessage<string, string>("key2", "value");
            var km3 = new KafkaMessage<string, string>("key3", "value");
            Assert.True(mbk.TryAddMessageToHandle(km1));
            Assert.False(mbk.TryAddMessageToHandle(km1));
            Assert.True(mbk.TryAddMessageToHandle(km2));

            IKafkaMessage<string, string> nextMessage;
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
            var mbk = new MessagesByKey<int, int>();
            var kms = Enumerable.Range(1, 50).Select(i => new KafkaMessage<int, int>(i % 10, i)).ToList();

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
