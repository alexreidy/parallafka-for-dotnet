using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Parallafka.Tests.Helpers
{
    public class ThreadSafeRandomTests
    {
        private readonly ITestOutputHelper _output;

        [Theory]
        [InlineData(100, 1000, 1)]
        [InlineData(100, 1000, 20)]
        public async Task HowLongAsync(int warmup, int iterations, int threadCount)
        {
            ThreadPool.GetMinThreads(out _, out var completionPortThreads);
            ThreadPool.SetMinThreads(threadCount * 2, completionPortThreads);

            var rng = new ThreadSafeRandom();

            for (var i = 0; i < warmup; i++)
            {
                await rng.BorrowAsync(r =>
                {
                    r.Next();
                    return Task.CompletedTask;
                });
            }

            var sr = new StaticRandom();
            for (var i = 0; i < warmup; i++)
            {
                sr.Next();
            }

            var sw = Stopwatch.StartNew();

            async Task RunBorrowAsync()
            {
                for (var i = 0; i < iterations; i++)
                {
                    await rng.BorrowAsync(r =>
                    {
                        r.Next();
                        return Task.CompletedTask;
                    });
                }
            }

            var threads = Enumerable.Range(1, threadCount)
                .Select(i => Task.Run(RunBorrowAsync));

            await Task.WhenAll(threads);

            sw.Stop();
            
            this._output.WriteLine($"ThreadSafeRandom.Async latency: {sw.Elapsed:g}");

            sw = Stopwatch.StartNew();

            void RunBorrow()
            {
                for (var i = 0; i < iterations; i++)
                {
                    rng.Borrow(r => { r.Next(); });
                }
            }

            threads = Enumerable.Range(1, threadCount)
                .Select(i => Task.Run(RunBorrow));

            await Task.WhenAll(threads);

            sw.Stop();

            this._output.WriteLine($"ThreadSafeRandom.Sync latency: {sw.Elapsed:g}");


            sw = Stopwatch.StartNew();

            void RunStatic()
            {
                for (var i = 0; i < iterations; i++)
                {
                    sr.Next();
                }
            }

            var actualThreads = Enumerable.Range(1, threadCount)
                .Select(i => new Thread(() => RunStatic()))
                .ToList();

            foreach (var thread in actualThreads)
            {
                thread.Start();
            }

            foreach (var thread in actualThreads)
            {
                thread.Join();
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
