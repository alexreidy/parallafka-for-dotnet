using System;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka
{
    /// <summary>
    /// Performs commits when needed, or on a delay
    /// </summary>
    internal class CommitPoller
    {
        private readonly IMessageCommitter _committer;
        private readonly CancellationTokenSource _cancellation;
        private readonly CancellationToken _cancellationToken;

        private string _state;
        private bool _complete;

        public CommitPoller(IMessageCommitter committer)
        {
            this._committer = committer;
            this._cancellation = new CancellationTokenSource();
            this._cancellationToken = _cancellation.Token;
            this._state = "starting";

            this.Completion = CommitLoop();
        }

        public object GetStats()
        {
            return new
            {
                State = this._state,
                TotalCommits = this._totalCommits,
                DelayedCommits = this._commitDelays,
                QueueFullCommits = this._commitNows
            };
        }

        public void Complete()
        {
            lock (this._taskLock)
            {
                this._complete = true;
            }

            this._cancellation.Cancel();
        }

        public Task Completion { get; }

        /// <summary>
        /// Returns true if the commit was queued, false if it was already queued or the poller is Complete
        /// </summary>
        public bool CommitWithin(TimeSpan delay)
        {
            lock (this._taskLock)
            {
                if (!this._complete)
                {
                    return this._commitDelay.TrySetResult(delay);
                }
            }

            return false;
        }

        /// <summary>
        /// Returns true if the commit was queued, false if it was already queued or the poller is Complete
        /// </summary>
        public bool CommitNow()
        {
            lock (this._taskLock)
            {
                if (!this._complete)
                {
                    return this._commitNow.TrySetResult();
                }
            }

            return false;
        }

        private readonly object _taskLock = new();
        private TaskCompletionSource _commitNow = new();
        private TaskCompletionSource<TimeSpan> _commitDelay = new();
        private long _totalCommits;
        private long _commitDelays;
        private long _commitNows;

        private async Task CommitLoop()
        {
            while (true)
            {
                this._state = "idle";

                var delayTask = Task.Delay(-1, this._cancellationToken);
                var result = await Task.WhenAny(
                    delayTask,
                    this._commitNow.Task,
                    this._commitDelay.Task);

                if (result == delayTask)
                {
                    // see if there's any work to be done before loop completion
                    if (this._commitNow.Task.IsCompletedSuccessfully || this._commitDelay.Task.IsCompletedSuccessfully)
                    {
                        await PerformCommit();
                    }

                    break;
                }
                
                if (result == this._commitNow.Task)
                {
                    Interlocked.Increment(ref this._commitNows);
                    lock (this._taskLock)
                    {
                        this._commitNow = new();
                    }

                }
                else if (result == this._commitDelay.Task)
                {
                    Interlocked.Increment(ref this._commitDelays);
                    TimeSpan duration;
                    lock (this._taskLock)
                    {
                        duration = this._commitDelay.Task.Result;
                        this._commitDelay = new();
                    }

                    this._state = $"waiting {duration:g}";

                    result = await Task.WhenAny(
                        this._commitNow.Task,
                        Task.Delay(duration, this._cancellationToken));

                    if (result == this._commitNow.Task)
                    {
                        Interlocked.Increment(ref this._commitNows);
                        lock (this._taskLock)
                        {
                            this._commitNow = new();
                        }
                    }
                }

                await PerformCommit();
            }

            this._state = "completed";
        }

        private Task PerformCommit()
        {
            this._state = "committing";
            Interlocked.Increment(ref this._totalCommits);
            return this._committer.CommitNow(default);
        }
    }
}
