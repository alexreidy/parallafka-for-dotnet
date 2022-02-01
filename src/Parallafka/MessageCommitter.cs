using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Parallafka.KafkaConsumer;

namespace Parallafka
{
    /// <summary>
    /// Commits offsets to Kafka
    /// </summary>
    /// <typeparam name="TKey">The key type</typeparam>
    /// <typeparam name="TValue">The value type</typeparam>
    internal class MessageCommitter<TKey, TValue> : IMessageCommitter
    {
        private readonly IKafkaConsumer<TKey, TValue> _consumer;
        private readonly IMessagesToCommit<TKey, TValue> _commitState;
        private readonly ILogger _logger;
        private readonly SemaphoreSlim _committerLock;

        private long _messagesCommitted;
        private long _messagesCommitErrors;
        private long _messagesCommitLoops;

        public MessageCommitter(
            IKafkaConsumer<TKey, TValue> consumer,
            IMessagesToCommit<TKey, TValue> commitState,
            ILogger logger)
        {
            this._committerLock = new SemaphoreSlim(1, 1);
            this._consumer = consumer;
            this._commitState = commitState;
            this._logger = logger;
        }

        public object GetStats()
        {
            return new
            {
                MessageCommitLoopInProgress = _committerLock.CurrentCount > 0,
                MessagesCommitted = this._messagesCommitted,
                MessagesCommitErrors = this._messagesCommitErrors,
                MessagesCommitLoops = this._messagesCommitLoops
            };
        }

        /// <summary>
        /// Commits any messages that can be committed
        /// </summary>
        public async Task CommitNow(CancellationToken cancellationToken)
        {
            await this._committerLock.WaitAsync(cancellationToken);
            try
            {
                var loop = Interlocked.Increment(ref this._messagesCommitLoops);
                Parallafka<TKey, TValue>.WriteLine($"GetAndCommitAnyMessages start call#{loop}");
                foreach (var message in this._commitState.GetMessagesToCommit())
                {
                    await CommitMessage(message, cancellationToken);
                }

                Parallafka<TKey, TValue>.WriteLine($"GetAndCommitAnyMessages finish call#{loop}");
            }
            finally
            {
                this._committerLock.Release();
            }
        }

        private async Task CommitMessage(KafkaMessageWrapped<TKey, TValue> messageToCommit, CancellationToken cancellationToken)
        {
            for(;;)
            {
                try
                {
                    Parallafka<TKey, TValue>.WriteLine($"MsgCommitter: committing {messageToCommit.Offset}");

                    cancellationToken.ThrowIfCancellationRequested();

                    // TODO: inject CancelToken for hard-stop strategy?
                    await this._consumer.CommitAsync(messageToCommit.Message);

                    Interlocked.Increment(ref this._messagesCommitted);
                    break;
                }
                catch (Exception e)
                {
                    Interlocked.Increment(ref this._messagesCommitErrors);
                    this._logger.LogError(e, "Error committing offsets");
                    await Task.Delay(99, cancellationToken);
                }
            }
        }
    }
}
