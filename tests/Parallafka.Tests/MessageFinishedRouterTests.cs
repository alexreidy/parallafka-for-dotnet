﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        public async Task MessagesAreSentToDataflowCorrectly()
        {
            // given
            var mbk = new MessagesByKey<string, string>();
            var mfr = new MessageFinishedRouter<string, string>(mbk);
            var totalMessages = 5;
            var messages = Enumerable.Range(1, totalMessages)
                .Select(i => new KafkaMessage<string, string>("key", "value", new RecordOffset(0, i)))
                .ToList();

            foreach (var message in messages)
            {
                mbk.TryAddMessageToHandle(message);
            }

            var sentMessageCount = 0;
            IRecordOffset lastOffset = null;
            var flow = new ActionBlock<IKafkaMessage<string, string>>(m =>
            {
                lastOffset = m.Offset;
                Interlocked.Increment(ref sentMessageCount);
            });

            mfr.MessagesToHandle.LinkTo(flow, new DataflowLinkOptions { PropagateCompletion = true });

            foreach (var message in messages)
            {
                await mfr.MessageHandlerFinished(message);
            }

            mfr.Complete();
            await mfr.Completion;

            mfr.MessagesToHandle.Complete();
            await flow.Completion;

            // then
            Assert.Equal(totalMessages - 1, sentMessageCount);
            Assert.Equal(messages.Last().Offset, lastOffset);
        }
    }
}