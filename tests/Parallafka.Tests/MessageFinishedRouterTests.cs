using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests
{
    public class MessageFinishedRouterTests
    {
        [Fact]
        public async Task SameKeyMessagesAreSentToDataflowCorrectly()
        {
            // given
            var mbk = new MessagesByKey<string, string>(default);
            var mfr = new MessageFinishedRouter<string, string>(mbk);
            var totalMessages = 5;
            var messages = Enumerable.Range(1, totalMessages)
                .Select(i => KafkaMessage.Create("key", "value", new RecordOffset(0, i)).Wrapped())
                .ToList();

            var sentMessageCount = 0;
            IRecordOffset lastOffset = null;
            var flow = new ActionBlock<KafkaMessageWrapped<string, string>>(m =>
            {
                lastOffset = m.Offset;
                Interlocked.Increment(ref sentMessageCount);
            });

            mfr.MessagesToHandle.LinkTo(flow, new DataflowLinkOptions { PropagateCompletion = true });

            // the first message should be handled
            Assert.True(mbk.TryAddMessageToHandle(messages[0]));
            
            // the remaining messages should be queued
            foreach (var message in messages.Skip(1))
            {
                Assert.False(mbk.TryAddMessageToHandle(message));
            }
            
            // finish all the messages
            foreach (var message in messages)
            {
                await mfr.MessageHandlerFinished(message);
            }

            mfr.Complete();
            await mfr.Completion;

            mfr.MessagesToHandle.Complete();
            await flow.Completion;

            // then
            // only the queued messages are sent to the flow when MessageHandlerFinished is called.
            Assert.Equal(totalMessages - 1, sentMessageCount);
            Assert.Equal(messages.Last().Offset, lastOffset);
        }
    }
}
