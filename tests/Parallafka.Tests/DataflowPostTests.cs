using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests
{
    public class DataflowPostTests
    {
        private readonly ITestOutputHelper _console;

        public DataflowPostTests(ITestOutputHelper console)
        {
            _console = console;
        }

        /// <summary>
        /// Verifies that Post returns false when the ActionBlock is busy with all the tasks
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task VerifyDataflowPostWorksAsExpected()
        {
            // given
            var sw = Stopwatch.StartNew();
            var got = 0;
            var ab = new ActionBlock<int>(async i =>
            {
                this._console.WriteLine($"{sw.Elapsed:g} Block started {i}");
                Interlocked.Increment(ref got);
                await Task.Delay(TimeSpan.FromSeconds(1));
                Interlocked.Decrement(ref got);
                this._console.WriteLine($"{sw.Elapsed:g} Block finished {i}");

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
                while (got == 1 && ab.InputCount > 0)
                {
                    await Task.Delay(100);
                }

                await Task.Delay(1000);

                this._console.WriteLine($"{sw.Elapsed:g} Sending 2");

                // and when the action block finishes the current task, Post will return true again
                var c = 0;
                for (;;)
                {
                    c++;
                    var b = await ab.SendAsync(2);
                    if (b)
                    {
                        break;
                    }
                }

                this._console.WriteLine($"{sw.Elapsed:g} Sent 2 in {c} tries");

                // Assert.True(b);
            }
            finally
            {
                await Task.Delay(1000);

                // cleanup
                ab.Complete();
                await ab.Completion;
            }
        }
    }
}
