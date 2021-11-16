using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka.Tests
{
    public class ThreadSafeRandom : IDisposable
    {
        private BlockingCollection<Random> _rngs = new BlockingCollection<Random>();

        private int _minRngCount;

        private CancellationTokenSource _shutdownCts = new CancellationTokenSource();

        private bool _pruneThreadIsRunning = false;

        private double _rngCollectionLockIsTaken = 0;

        public ThreadSafeRandom(int minRngCount = 10)
        {
            this._minRngCount = minRngCount;
            for (int i = 0; i < _minRngCount; i++)
            {
                this._rngs.Add(new Random());
            }

            Task.Run(async () =>
            {
                this._pruneThreadIsRunning = true;
                while (!this._shutdownCts.Token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(15000, this._shutdownCts.Token);
                        this.WithLockOnRngCollection(() =>
                        {
                            if (this._rngs.Count > this._minRngCount)
                            {
                                // Remove the RNG and let it get GC'd
                                this._rngs.Take();
                            }
                        });
                    }
                    catch (TaskCanceledException)
                    {
                    }
                }
                this._pruneThreadIsRunning = false;
            });
        }

        public void Dispose()
        {
            this._shutdownCts.Cancel();

            while (this._pruneThreadIsRunning)
            {
                Thread.Sleep(2);
            }
        }

        public async Task BorrowAsync(Func<Random, Task> useAsync)
        {
            Random rng = null;
            this.WithLockOnRngCollection(() =>
            {
                if (!this._rngs.TryTake(out rng))
                {
                    rng = new Random();
                }
            });

            try
            {
                await useAsync(rng);
            }
            finally
            {
                this.WithLockOnRngCollection(() =>
                {
                    this._rngs.Add(rng);
                });
            }
        }

        public void Borrow(Action<Random> use)
        {
            Random rng = null;
            this.WithLockOnRngCollection(() =>
            {
                if (!this._rngs.TryTake(out rng))
                {
                    rng = new Random();
                }
            });

            try
            {
                use(rng);
            }
            finally
            {
                this.WithLockOnRngCollection(() =>
                {
                    this._rngs.Add(rng);
                });
            }
        }

        /// <summary></summary>
        /// <param name="act"></param>
        /// <returns>False if the lock was not available</returns>
        private bool WithLockOnRngCollectionIfAvailable(Action act)
        {
            if (Interlocked.CompareExchange(ref this._rngCollectionLockIsTaken, value: 1, comparand: 0) == 0)
            {
                return false;
            }

            try
            {
                act.Invoke();
                return true;
            }
            finally
            {
                this._rngCollectionLockIsTaken = 0;
            }
        }

        private void WithLockOnRngCollection(Action act)
        {
            while (!WithLockOnRngCollectionIfAvailable(act))
            {
                Thread.Sleep(2);
            }
        }
    }
}