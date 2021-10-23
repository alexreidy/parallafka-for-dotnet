# Parallafka for .NET

Parallafka is a parallelization layer over your out-of-the-box, one-message-at-a-time Kafka consumer, with built-in support for the [Confluent Kafka client](https://github.com/confluentinc/confluent-kafka-dotnet).

```cs
static async Task Main(string[] args)
{
    IKafkaConsumer<string, StockPrice> consumer = ConsumerForTopic<StockPrice>("ultimate-stonks-watchlist");

    IParallafkaConfig<string, StockPrice> config = new ParallafkaConfig<string, StockPrice>()
    {
        MaxConcurrentHandlers = 7,
    };
    IParallafka<string, StockPrice> parallafka = new Parallafka<string, StockPrice>(consumer, config);
    
    await parallafka.ConsumeAsync(async (IKafkaMessage<string, StockPrice> message) =>
    {
        // This handler code will process up to 7 messages at the same time
        Console.WriteLine($"{message.Value.TickerSymbol} is ${message.Value.Price}");
    });
}

static IKafkaConsumer<string, T> ConsumerForTopic<T>(string topicName)
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

    return new ConfluentConsumerAdapter<string, T>(rawConsumer, topicName);
}
```

In many cases, you don't need your Kafka consumer to process a partition in full FIFO order: It's not a problem that an email is sent to User2 before User1 despite User1 having registered 30 milliseconds earlier than User2 and being first in the topic. When this is the case, it's safe to parallelize handler code far beyond Kafka's 1-consumer-per-partition guarantee by polling the topic in the traditional order on one thread and then distributing the messages across handler threads to parallelize the actual processing. Parallafka does this for you, multiplying handler throughput significantly, while also diligently preserving the same-key order guarantee when you have, say, a message activating a feature for User1 and in quick succession a message deactivating the feature or the entire user. Parallafka ensures that, in the midst of all the parallelism, FIFO-order serial processing is preserved for any set of messages published with a shared key.

## Features
- Configurable handler parallelism.
- Preserves FIFO-order serial processing for messages with the same key, no matter how many concurrent handlers are running.
- Adapter supporting Confluent's Kafka consumer.
- Configurable shutdown behavior: graceful (wait to finish handling and commit, with timeout) and hard-stop.
- Injectable ILogger.

## Planned features
- Optional automatic performance tuning with adaptive handler thread count.
- Configuration options for commit behavior and frequency.

## Installation
```
dotnet add package Parallafka
dotnet add package Parallafka.AdapterForConfluentConsumer
```