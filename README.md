# Kafka Dataflow
An extension of [Confluent's Kafka client](https://github.com/confluentinc/confluent-kafka-dotnet) for use with `System.Threading.Tasks.Dataflow`.

## Features
* Represent consumers/producers as dataflow blocks.
* Process Kafka messages using a dataflow pipeline.

## Installation

Add the NuGet package to your project:

    $ dotnet add package kafka-dataflow

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
var source = consumer.AsSourceBlock(new ConsumeBlockOptions
{
    // It's a good idea to limit buffered messages (in case processing falls behind).
    // Otherwise, all messages are offered as soon as they are available.
    BoundedCapacity = 8,
});

source.LinkTo(processor, new DataflowLinkOptions { PropagateCompletion = true });

// Optionally, request to stop processing.
await Task.Delay(10000);
source.Complete();

// Wait for processing to finish.
await processor.Completion;
consumer.Close();
```

### Mapping messages to offsets

Sometimes we want to process messages alongside their corresponding offsets. This can be achieved using `JoinBlock<T1, T2>`:

```c#
// Define a target for offset/message pairs.
var processor = new ActionBlock<Tuple<TopicPartitionOffset, Message<string, string>>>(
    pair => Console.WriteLine($"Received message: {pair.Item1}"));

// Set up a block to join offsets to messages
var joiner = new JoinBlock<TopicPartitionOffset, Message<string, string>>();
var source = consumer.AsSourceBlock(new ConsumeBlockOptions { OffsetTarget = joiner.Target1 });

var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
source.LinkTo(joiner.Target2, linkOptions);
joiner.LinkTo(processor, linkOptions);
```

### Committing offsets

The Kafka client auto-commits periodically by default. If `enable.auto.offset.store` is configured to `false` (recommended), you can use `IConsumer<TKey, TValue>.AsOffsetBlock(...)` to control when offsets are stored/queued for the next commit.

You can store offsets at the time their messages leave the source block ("consumed" by a target):

```c#
var offsetTarget = consumer.AsOffsetBlock();
var source = consumer.AsSourceBlock(new ConsumerBlockOptions { OffsetTarget = offsetTarget });
```

Or store them later on in the pipeline (after processing is finished):

```c#
// Set up a join block for offset/message pairs (see above).
// ...

// Use a transform block to emit processed offsets.
var processor = new TransformBlock<Tuple<TopicPartitionOffset, Message<string, string>>, TopicPartitionOffset>(
    async pair =>
    {
        // Process message asynchronously.
        // ...
        return pair.Item1;
    },
    new ExecutionDataflowBlockOptions
    {
        // Parallelism is OK as long as order is preserved.
        MaxDegreeOfParallelism = 8,
        EnsureOrdered = true,
    });

// Link processor to offset target.
var offsetTarget = consumer.AsOffsetBlock();
processor.LinkTo(offsetTarget, linkOptions);
joiner.LinkTo(processor, linkOptions);
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
