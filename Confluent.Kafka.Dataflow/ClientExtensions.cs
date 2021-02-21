namespace Confluent.Kafka.Dataflow
{
    using System.Collections.Generic;
    using System.Threading.Tasks.Dataflow;
    using Confluent.Kafka.Dataflow.Internal;

    /// <summary>
    /// Extensions to represent Kafka producers and consumers as dataflow blocks.
    /// </summary>
    public static class ClientExtensions
    {
        /// <summary>
        /// Represents a producer as a target block for Kafka key/value pairs.
        /// </summary>
        /// <remarks>
        /// A producer can be represented as multiple targets.
        /// </remarks>
        /// <typeparam name="TKey">The producer key type.</typeparam>
        /// <typeparam name="TValue">The producer value type.</typeparam>
        /// <param name="producer">The producer.</param>
        /// <param name="topicPartition">The topic/partition receiving the messages. Use <see cref="Partition.Any"/> for automatic partitioning.</param>
        /// <returns>The producer target block.</returns>
        public static ITargetBlock<KeyValuePair<TKey, TValue>> AsTarget<TKey, TValue>(
            this IProducer<TKey, TValue> producer,
            TopicPartition topicPartition)
        {
            return new ProducerBlockFactory<TKey, TValue>(producer).GetTarget(topicPartition);
        }

        /// <summary>
        /// Represents a consumer as a source block for Kafka key/value pairs.
        /// </summary>
        /// <remarks>
        /// Consumers must be subscribed/assigned in order to produce data.
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <returns>The consumer source block.</returns>
        public static ISourceBlock<KeyValuePair<TKey, TValue>> AsSource<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer)
        {
            return new ConsumerBlockFactory<TKey, TValue>(consumer).GetSource();
        }
    }
}
