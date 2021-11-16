using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Moq;
using Parallafka.KafkaConsumer;
using Xunit;

namespace Parallafka.Tests
{
    public class MessageRouterTests
    {
        [Fact]
        public async Task VerifyMessageRoutedOnlyOnce()
        {
            // given
            var cs = new CommitState<string, string>();
            var mbk = new MessagesByKey<string, string>();
            var mr = new MessageRouter<string, string>(cs, mbk, default);
            var message1 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 0));
            var message2 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 1));
            var messageCount = 0;
            var flow = new ActionBlock<IKafkaMessage<string, string>>(m =>
            {
                Interlocked.Increment(ref messageCount);
            });

            mr.MessagesToHandle.LinkTo(flow, new DataflowLinkOptions { PropagateCompletion = true });
            await mr.RouteMessage(message1);

            // when
            await mr.RouteMessage(message2);

            mr.MessagesToHandle.Complete();
            await flow.Completion;

            // then
            Assert.Equal(1, messageCount);
        }

        [Fact]
        public async Task VerifyMessageRoutedAfterMessageCleared()
        {
            // given
            var cs = new CommitState<string, string>();
            var mbk = new MessagesByKey<string, string>();
            var mr = new MessageRouter<string, string>(cs, mbk, default);
            var message1 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 0));
            var message2 = new KafkaMessage<string, string>("key", "value", new RecordOffset(0, 1));
            var messageCount = 0;
            var flow = new ActionBlock<IKafkaMessage<string, string>>(m =>
            {
                Interlocked.Increment(ref messageCount);
            });

            mr.MessagesToHandle.LinkTo(flow, new DataflowLinkOptions { PropagateCompletion = true });
            await mr.RouteMessage(message1);
            
            // when
            mbk.TryGetNextMessageToHandle(message1, out _);
            await mr.RouteMessage(message2);

            mr.MessagesToHandle.Complete();
            await flow.Completion;

            // then
            Assert.Equal(2, messageCount);
        }
    }
}
