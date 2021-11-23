using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;
using Parallafka;
using Parallafka.Adapters.ConfluentKafka;
using Parallafka.KafkaConsumer;

namespace BasicConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string topicName = "ultimate-stonks-watchlist";
            IConsumer<string, StockPrice> confluentConsumer = ConsumerForTopic<StockPrice>(topicName);
            IKafkaConsumer<string, StockPrice> consumer = new ConfluentConsumerAdapter<string, StockPrice>(
                confluentConsumer, topicName);

            IParallafkaConfig<string, StockPrice> config = new ParallafkaConfig<string, StockPrice>()
            {
                MaxConcurrentHandlers = 7
            };
            IParallafka<string, StockPrice> parallafka = new Parallafka<string, StockPrice>(consumer, config);

            await parallafka.ConsumeAsync(async (IKafkaMessage<string, StockPrice> message) =>
            {
                Console.WriteLine($"{message.Value.TickerSymbol} is ${message.Value.Price}");
            });
        }

        static IConsumer<string, T> ConsumerForTopic<T>(string topicName)
            where T : class
        {
            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9092",
                GroupId = "stonks-autotrader",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };
            var consumerBuilder = new ConsumerBuilder<string, T>(consumerConfig)
                .SetValueDeserializer(new JsonDeserializer<T>().AsSyncOverAsync());
            IConsumer<string, T> rawConsumer = consumerBuilder.Build();
            rawConsumer.Subscribe(topicName);
            return rawConsumer;
        }
    }

    class StockPrice
    {
        public string TickerSymbol { get; set; }

        public decimal Price { get; set; }
    }
}
