namespace Confluent.Kafka.Dataflow
{
    using System;
    using System.Threading.Tasks.Dataflow;
    using Confluent.Kafka.Dataflow.Internal;

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
        /// <param name="options">Block options for consuming.</param>
        /// <returns>The consumer source block.</returns>
        public static ISourceBlock<Message<TKey, TValue>> AsSourceBlock<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer,
            ConsumeBlockOptions? options = null)
        {
            return new ConsumeBlock<TKey, TValue>(
                consumer ?? throw new ArgumentNullException(nameof(consumer)),
                options ?? new());
        }

        /// <summary>
        /// Represents a consumer as a target block for processed Kafka offsets.
        /// </summary>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <param name="options">Block options for processing.</param>
        /// <returns>The consumer offset block.</returns>
        public static ITargetBlock<TopicPartitionOffset> AsOffsetBlock<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer,
            OffsetBlockOptions? options = null)
        {
            if (consumer == null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            return new ActionBlock<TopicPartitionOffset>(
                x => consumer.StoreOffset(new TopicPartitionOffset(x.TopicPartition, x.Offset + 1)),
                options ?? new());
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
        /// <param name="topicPartition">The topic/partition receiving the messages. Use <see cref="Partition.Any"/> for automatic partitioning.</param>
        /// <param name="options">Block options for producing.</param>
        /// <returns>The producer target block.</returns>
        public static ITargetBlock<Message<TKey, TValue>> AsTargetBlock<TKey, TValue>(
            this IProducer<TKey, TValue> producer,
            TopicPartition topicPartition,
            ProduceBlockOptions? options = null)
        {
            return new ProduceBlock<TKey, TValue>(
                producer ?? throw new ArgumentNullException(nameof(producer)),
                topicPartition ?? throw new ArgumentNullException(nameof(topicPartition)),
                options ?? new ProduceBlockOptions());
        }
    }
}
