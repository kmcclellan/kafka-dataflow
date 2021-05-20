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
