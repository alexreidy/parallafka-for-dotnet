using System.Collections.Generic;
using System.Threading.Tasks;
using Parallafka.KafkaConsumer;

namespace Parallafka.Tests
{
    public interface ITestKafkaTopic
    {
        /// <summary>
        /// Returns a consumer instance for the given consumer group ID.
        /// </summary>
        Task<IKafkaConsumer<string, string>> GetConsumerAsync(string groupId);
        
        /// <summary>
        /// Publishes the given messages to the test topic in order.
        /// </summary>
        Task PublishAsync(IEnumerable<IKafkaMessage<string, string>> messages);

        Task DeleteAsync();
    }
}