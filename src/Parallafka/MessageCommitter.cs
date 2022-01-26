using System;
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

        public MessageCommitter(
            IKafkaConsumer<TKey, TValue> consumer,
            CommitState<TKey, TValue> commitState,
            ILogger logger)
        {
            this._consumer = consumer;
            this._commitState = commitState;
            this._logger = logger;
        }

        /// <summary>
        /// Commits any messages that can be committed
        /// </summary>
        public Task CommitNow(CancellationToken cancellationToken)
        {
            return this.GetAndCommitAnyMessages(cancellationToken);
        }

        /// <summary>
        /// Gets any possible messages to commit and commits them
        /// </summary>
        /// <returns></returns>
        private async Task GetAndCommitAnyMessages(CancellationToken cancellationToken)
        {
            Parallafka<TKey, TValue>.WriteLine("GetAndCommitAnyMessages start");
            foreach (var message in this._commitState.GetMessagesToCommit())
            {
                await CommitMessage(message, cancellationToken);
            }

            Parallafka<TKey, TValue>.WriteLine("GetAndCommitAnyMessages finish");
        }

        private async Task CommitMessage(IKafkaMessage<TKey, TValue> messageToCommit, CancellationToken cancellationToken)
        {
            for(;;)
            {
                try
                {
                    Parallafka<TKey, TValue>.WriteLine($"MsgCommitter: committing {messageToCommit.Offset}");

                    cancellationToken.ThrowIfCancellationRequested();

                    // TODO: inject CancelToken for hard-stop strategy?
                    await this._consumer.CommitAsync(messageToCommit);

                    break;
                }
                catch (Exception e)
                {
                    this._logger.LogError(e, "Error committing offsets");
                    await Task.Delay(99, cancellationToken);
                }
            }
        }
    }
}
