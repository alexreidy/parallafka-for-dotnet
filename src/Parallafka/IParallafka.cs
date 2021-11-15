using System;
using System.Threading;
using System.Threading.Tasks;

namespace Parallafka
{
    public interface IParallafka<TKey, TValue>
    {
        /// <summary>
        /// Continues consuming messages until the stopToken is cancelled
        /// </summary>
        /// <param name="stopToken"></param>
        /// <returns>A task that is not completed until the ConsumeAsync method has completed</returns>
        Task ConsumeAsync(CancellationToken stopToken);
    }
}