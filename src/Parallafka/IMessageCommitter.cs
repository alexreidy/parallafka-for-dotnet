using System.Threading;
using System.Threading.Tasks;

namespace Parallafka
{
    internal interface IMessageCommitter
    {
        /// <summary>
        /// Commits any messages that can be committed
        /// </summary>
        Task CommitNow(CancellationToken cancellationToken);
    }
}