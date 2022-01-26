using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace Parallafka.Tests
{
    public class DataflowPostTests
    {
        /// <summary>
        /// Verifies that Post returns false when the ActionBlock is busy with all the tasks
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task VerifyDataflowPostWorksAsExpected()
        {
            // given
            var got = 0;
            var ab = new ActionBlock<int>(async i =>
            {
                Interlocked.Increment(ref got);
                await Task.Delay(TimeSpan.FromSeconds(1));
                Interlocked.Decrement(ref got);

            }, new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = 1,
                MaxDegreeOfParallelism = 1
            });

            try
            {
                Assert.True(ab.Post(1));
                while (got != 1)
                {
                    await Task.Yield();
                }

                // when the action block is busy with the single current task, Post will return false
                Assert.False(ab.Post(1));
                while (got == 1)
                {
                    await Task.Delay(100);
                }

                // and when the action block finishes the current task, Post will return true again
                Assert.True(ab.Post(1));

            }
            finally
            {
                // cleanup
                ab.Complete();
                await ab.Completion;
            }
        }
    }
}
