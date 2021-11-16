using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Helpers
{
    public class ThreadSafeRandomTests
    {
        private readonly ITestOutputHelper _output;

        [Theory]
        [InlineData(100, 1000)]
        public async Task HowLongAsync(int warmup, int iterations)
        {
            var rng = new ThreadSafeRandom();

            for (var i = 0; i < warmup; i++)
            {
                await rng.BorrowAsync(r =>
                {
                    r.Next();
                    return Task.CompletedTask;
                });
            }

            var sw = Stopwatch.StartNew();

            for (var i = 0; i < iterations; i++)
            {
                await rng.BorrowAsync(r =>
                {
                    r.Next();
                    return Task.CompletedTask;
                });
            }

            sw.Stop();
            
            this._output.WriteLine($"ThreadSafeRandom latency: {sw.Elapsed:g}");

            var sr = new StaticRandom();
            for (var i = 0; i < warmup; i++)
            {
                sr.Next();
            }

            sw = Stopwatch.StartNew();

            for (var i = 0; i < iterations; i++)
            {
                sr.Next();
            }

            sw.Stop();

            this._output.WriteLine($"StaticRandom latency: {sw.Elapsed:g}");
        }

        private class StaticRandom
        {
            private readonly Random _rnd = new();

            public int Next()
            {
                lock (this._rnd)
                {
                    return _rnd.Next();
                }
            }
        }

        public ThreadSafeRandomTests(ITestOutputHelper output)
        {
            this._output = output;
        }
    }
}
