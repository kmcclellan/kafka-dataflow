# Kafka Dataflow
An extension of [Confluent's Kafka client](https://github.com/confluentinc/confluent-kafka-dotnet) for use with `System.Threading.Tasks.Dataflow`.

## Features
* Represent consumers/producers as dataflow blocks.
* Process Kafka messages using a dataflow pipeline.
* Configure transactional producing and EoS stream processing.

## Installation

Add the NuGet package to your project:

    $ dotnet add package Confluent.Kafka.Dataflow
    
## Usage

### Consuming using `ISourceBlock<T>`

Use `IConsumer<TKey,TValue>.AsSourceBlock(...)` to initialize a Kafka message pipeline.

```c#
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Confluent.Kafka.Dataflow;

using var consumer = new ConsumerBuilder<string, string>(
    new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "example",
    }).Build();

consumer.Subscribe("my-topic");

// Define a target block to process Kafka messages.
var processor = new ActionBlock<Message<string, string>>(
    message => Console.WriteLine($"Message received: {message.Timestamp}"));

// Initialize source and link to target.
var blockOptions = new DataflowBlockOptions
{
    // It's a good idea to limit buffered messages (in case processing falls behind).
    // Otherwise, all messages are offered as soon as they are available.
    BoundedCapacity = 8,
};

var source = consumer.AsSourceBlock(options: blockOptions);
source.LinkTo(processor, new DataflowLinkOptions { PropagateCompletion = true });

// Optionally, request to stop processing.
await Task.Delay(10000);
source.Complete();

// Wait for processing to finish.
await processor.Completion;
consumer.Close();
```

### Committing message offsets

The Kafka client auto-commits periodically by default. It can automatically store/queue messages for the next commit as soon as they are loaded into memory.

Alternatively, you can set `enable.auto.offset.store` to `false` and store offsets manually after processing is finished. This prevents unprocessed messages from being committed in exceptional scenarios.

```c#

// Use a transform block to emit processed messages.
var processor = new TransformBlock<Message<string, string>, Message<string, string>>(
    async message =>
    {
        // Process message asynchronously.
        // ...
        return message;
    },
    new ExecutionDataflowBlockOptions
    {
        // Parallelism is OK as long as order is preserved.
        MaxDegreeOfParallelism = 8,
        EnsureOrdered = true,
    });

// Link the processor to the source and commit target.
var source = consumer.AsSourceBlock(out commitTarget, options: blockOptions);

var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
source.LinkTo(processor, linkOptions);
processor.LinkTo(commitTarget, linkOptions);
```

### Producing using `ITargetBlock<T>`

Use `IProducer<TKey, TValue>.AsTargetBlock(...)` to direct a message pipeline into a destination Kafka topic:

```c#
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Confluent.Kafka.Dataflow;

using var producer = new ProducerBuilder<string, string>(
    new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
    }).Build();

var target = producer.AsTargetBlock(new TopicPartition("my-topic", Partition.Any));

var generator = new TransformBlock<int, Message<string, string>>(
    i => new Message<string, string>
    {
        Key = i.ToString(),
        Value = $"Value #{i}"
    });

generator.LinkTo(target, new DataflowLinkOptions { PropagateCompletion = true });

for (var i = 0; i < 10; i++)
{
    generator.Post(i);
}

generator.Complete();
await target.Completion;
```

### Transactions and stream processing using `TargetBuilder<T>`

Kafka supports producing messages and committing offsets in transactions. A common use case is for "stream processing," so a processed message offset is committed atomically with the corresponding messages it produced ("exactly once semantics").

Representing all data you want included within the same transaction as a custom data type, you can use `TargetBuilder<T>` to create a single composite target:

```c#

// Payment and shipping requests should be created transactionally.
class OrderData
{
    public OrderData(PaymentRequest payment, ShippingRequest shipping)
    {
        this.Payment = payment;
        this.Shipping = shipping;
    }

    public PaymentRequest Payment { get; }

    public ShippingRequest Shipping { get; }
}

// Configure mappings to Kafka messages to be produced.
var builder = new TargetBuilder<OrderData>()
    .WithMessages(
        myProducer,
        order => new[] { new Message<Null, PaymentRequest> { Value = order.Payment } },
        new TopicPartition("payment-requests", Partition.Any))
    .WithMessages(
        myProducer,
        order => new[] { new Message<Null, ShippingRequest> { Value = order.Shipping } },
        new TopicPartition("shipping-requests", Partition.Any));

// Optionally, link a Kafka source to include the processed offset for each order.
// Payment and shipping will be created if and only if the order is committed.
builder.AsStream(myConsumer, out var orderSource)
var orderProcessor = new TransformBlock<OrderRequest, OrderData>(this.ProcessOrder);
orderSource.LinkTo(orderProcessor, LinkOptions);

// Finish linking Kafka stream. 
var orderTarget = builder.Build();
orderProcessor.LinkTo(orderTarget, LinkOptions);

await orderTarget.Completion;
```
