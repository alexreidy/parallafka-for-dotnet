using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    internal class MessageCommitter<TKey, TValue>
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;
        private readonly CommitState<TKey, TValue> _commitState;
        private readonly ILogger _logger;
        private readonly CancellationToken _stopToken;
        private readonly CancellationTokenSource _stopTimer;
        private readonly Task _commitTimer;
        private readonly Dictionary<int, CommitPartitionState> _committedStates;

        public MessageCommitter(
            IKafkaConsumer<TKey, TValue> consumer,
            CommitState<TKey, TValue> commitState,
            ILogger logger,
            TimeSpan timerDelay,
            CancellationToken stopToken)
        {
            this._committedStates = new();
            this._consumer = consumer;
            _commitState = commitState;
            this._logger = logger;
            this._stopToken = stopToken;
            this.Completion = new TaskCompletionSource().Task;
            this._stopTimer = new();
            this._commitTimer = Task.Run(() => this.CommitOnTimer(timerDelay));
        }

        /// <summary>
        /// Indicates that the message committer should complete all possible commits.
        /// Await <see cref="Completion"/> to know when the committer is completed.
        /// </summary>
        public void Complete()
        {
            this._stopTimer.Cancel();
            Completion = Task.WhenAll(this._commitTimer, GetAndCommitAnyMessages());
        }

        /// <summary>
        /// A task that when completed indicates the committer is finished processing
        /// </summary>
        public Task Completion { get; private set; }

        /// <summary>
        /// Attempts to commit up to the message's offset
        /// </summary>
        /// <param name="message">The message to commit</param>
        /// <returns>True if the message was committed, false if it was queued to be committed later</returns>
        public async Task<bool> TryCommitMessage(IKafkaMessage<TKey, TValue> message)
        {
            if (!this._commitState.TryGetMessageToCommit(message, out IKafkaMessage<TKey, TValue> messageToCommit))
            {
                Parallafka<TKey, TValue>.WriteLine($"MsgCommitter: {message.Offset} skipping");
                return false;
            }

            await CommitMessage(messageToCommit);
            return messageToCommit == message;
        }

        /// <summary>
        /// Gets any possible messages to commit and commits them
        /// </summary>
        /// <returns></returns>
        private async Task GetAndCommitAnyMessages()
        {
            var messages = this._commitState.GetMessagesToCommit().ToList();

            foreach (var message in messages)
            {
                await CommitMessage(message);
            }
        }

        /// <summary>
        /// Commits any messages on a timer delay
        /// </summary>
        /// <param name="delay"></param>
        /// <returns></returns>
        private async Task CommitOnTimer(TimeSpan delay)
        {
            for (; ; )
            {
                try
                {
                    await Task.Delay(delay, this._stopTimer.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                await GetAndCommitAnyMessages();
            }
        }

        private async Task CommitMessage(IKafkaMessage<TKey, TValue> messageToCommit)
        {
            while (!this._stopToken.IsCancellationRequested)
            {
                try
                {
                    CommitPartitionState state;
                    lock (this._committedStates)
                    {
                        if (!this._committedStates.TryGetValue(messageToCommit.Offset.Partition, out state))
                        {
                            this._committedStates[messageToCommit.Offset.Partition] = state = new();
                        }
                    }

                    // commit per partition from one task at a time
                    await state.Lock.WaitAsync(this._stopToken);
                    try
                    {
                        if (state.Offset >= messageToCommit.Offset.Offset)
                        {
                            break;
                        }

                        Parallafka<TKey, TValue>.WriteLine($"MsgCommitter: committing {messageToCommit.Offset}");

                        // TODO: inject CancelToken for hard-stop strategy?
                        await this._consumer.CommitAsync(messageToCommit.Offset);
                        state.Offset = messageToCommit.Offset.Offset;
                    }
                    finally
                    {
                        state.Lock.Release();
                    }

                    break;
                }
                catch (Exception e)
                {
                    this._logger.LogError(e, "Error committing offsets");
                    await Task.Delay(99);
                }
            }
        }

        private class CommitPartitionState
        {
            public SemaphoreSlim Lock { get; } = new(1);

            public long Offset { get; set; } = -1;
        }
    }
}
