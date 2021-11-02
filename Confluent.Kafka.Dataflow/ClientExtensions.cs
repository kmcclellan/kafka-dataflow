namespace Confluent.Kafka.Dataflow
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using Confluent.Kafka.Dataflow.Blocks;
    using Confluent.Kafka.Dataflow.Consuming;

    /// <summary>
    /// Extensions to represent Kafka producers and consumers as dataflow blocks.
    /// </summary>
    public static class ClientExtensions
    {
        /// <summary>
        /// Represents a consumer as a source block for Kafka messages.
        /// </summary>
        /// <remarks>
        /// Consumer must be subscribed/assigned for messages to be received.
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <param name="handler">A delegate invoked for each Kafka message (before offering to targets).</param>
        /// <param name="options">Block options for consuming.</param>
        /// <returns>The consumer source block.</returns>
        public static ISourceBlock<Message<TKey, TValue>> AsSourceBlock<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer,
            Action<Message<TKey, TValue>, TopicPartitionOffset>? handler = null,
            DataflowBlockOptions? options = null)
        {
            var loader = new MessageLoader<TKey, TValue>(consumer ?? throw new ArgumentNullException(nameof(consumer)));
            loader.OnConsumed += handler;

            return new CustomBlock<Message<TKey, TValue>>(loader.Load, options ?? new());
        }

        /// <summary>
        /// Represents a consumer as a source block for Kafka messages, with a linked commit target.
        /// </summary>
        /// <remarks>
        /// Consumer must be subscribed/assigned and have <c>enable.auto.offset.store</c> set to false.
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <param name="commitTarget">
        /// A target block for committing processed messages. Order must be preserved.
        /// </param>
        /// <param name="handler">A delegate invoked for each Kafka message (before offering to targets).</param>
        /// <param name="options">Block options for consuming.</param>
        /// <returns>The consumer source block.</returns>
        public static ISourceBlock<Message<TKey, TValue>> AsSourceBlock<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer,
            out ITargetBlock<Message<TKey, TValue>> commitTarget,
            Action<Message<TKey, TValue>, TopicPartitionOffset>? handler = null,
            DataflowBlockOptions? options = null)
        {
            var buffer = new ConcurrentQueue<(Message<TKey, TValue>, TopicPartitionOffset)>();

            var loader = new MessageLoader<TKey, TValue>(consumer ?? throw new ArgumentNullException(nameof(consumer)));
            loader.OnConsumed += handler + Store;

            void Store(Message<TKey, TValue> message, TopicPartitionOffset offset)
            {
                buffer.Enqueue((message, offset));
            }

            options ??= new();

            var target = new ActionBlock<Message<TKey, TValue>>(
                message =>
                {
                    if (!buffer.TryDequeue(out var stored) || stored.Item1 != message)
                    {
                        throw new InvalidOperationException("Unexpected message.");
                    }

                    consumer.StoreOffset(
                        new TopicPartitionOffset(stored.Item2.TopicPartition, stored.Item2.Offset + 1));
                },
                new()
                {
                    BoundedCapacity = options.BoundedCapacity,
                    CancellationToken = options.CancellationToken,
                    EnsureOrdered = options.EnsureOrdered,
                    NameFormat = options.NameFormat,
                    MaxMessagesPerTask = options.MaxMessagesPerTask,
                    TaskScheduler = options.TaskScheduler,
                    MaxDegreeOfParallelism = 1,
                    SingleProducerConstrained = true,
                });

            commitTarget = new ContinueBlock<Message<TKey, TValue>>(
                target,
                () => Task.Factory.StartNew(
                    consumer.Commit,
                    options.CancellationToken,
                    TaskCreationOptions.LongRunning,
                    options.TaskScheduler),
                options);

            return new CustomBlock<Message<TKey, TValue>>(loader.Load, options);
        }

        /// <summary>
        /// Represents a producer as a target block for Kafka messages.
        /// </summary>
        /// <remarks>
        /// A producer can be represented as multiple targets.
        /// </remarks>
        /// <typeparam name="TKey">The producer key type.</typeparam>
        /// <typeparam name="TValue">The producer value type.</typeparam>
        /// <param name="producer">The producer.</param>
        /// <param name="topicPartition">
        /// The topic/partition receiving the messages. Use <see cref="Partition.Any"/> for automatic partitioning.
        /// </param>
        /// <param name="handler">A delegate invoked for each delivered message, with its offset.</param>
        /// <param name="options">Block options for producing.</param>
        /// <returns>The producer target block.</returns>
        public static ITargetBlock<Message<TKey, TValue>> AsTargetBlock<TKey, TValue>(
            this IProducer<TKey, TValue> producer,
            TopicPartition topicPartition,
            Action<Message<TKey, TValue>, TopicPartitionOffset>? handler = null,
            DataflowBlockOptions? options = null)
        {
            if (producer == null)
            {
                throw new ArgumentNullException(nameof(producer));
            }

            if (topicPartition == null)
            {
                throw new ArgumentNullException(nameof(topicPartition));
            }

            options ??= new();

            return new ActionBlock<Message<TKey, TValue>>(
                async message =>
                {
                    var result = await producer.ProduceAsync(topicPartition, message).ConfigureAwait(false);
                    handler?.Invoke(result.Message, result.TopicPartitionOffset);
                },
                new()
                {
                    BoundedCapacity = options.BoundedCapacity,
                    CancellationToken = options.CancellationToken,
                    EnsureOrdered = options.EnsureOrdered,
                    NameFormat = options.NameFormat,
                    MaxMessagesPerTask = options.MaxMessagesPerTask,
                    TaskScheduler = options.TaskScheduler,
                    MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded,
                    SingleProducerConstrained = false,
                });
        }
    }
}
