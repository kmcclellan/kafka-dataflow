namespace Confluent.Kafka.Dataflow
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks.Dataflow;
    using Confluent.Kafka.Dataflow.Internal;

    /// <summary>
    /// Extensions to represent Kafka producers and consumers as dataflow blocks.
    /// </summary>
    public static class ClientExtensions
    {
        readonly static ClientCache<ProducerBlockFactory> producerFactories = new();
        readonly static ClientCache<ConsumerBlockFactory> consumerFactories = new();

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
            if (producer == null)
            {
                throw new ArgumentNullException(nameof(producer));
            }

            if (topicPartition == null)
            {
                throw new ArgumentNullException(nameof(topicPartition));
            }

            return producerFactories.GetOrAdd(producer).GetTarget(producer, topicPartition);
        }

        /// <summary>
        /// Represents a producer as a source block for delivered Kafka offsets.
        /// </summary>
        /// <remarks>
        /// For transactional producers, messages include all produced and committed offsets in a transaction.
        /// </remarks>
        /// <typeparam name="TKey">The producer key type.</typeparam>
        /// <typeparam name="TValue">The producer value type.</typeparam>
        /// <param name="producer">The producer.</param>
        /// <returns>The offset source block.</returns>
        public static IReceivableSourceBlock<IReadOnlyList<TopicPartitionOffset>> AsOffsetSource<TKey, TValue>(
            this IProducer<TKey, TValue> producer)
        {
            if (producer == null)
            {
                throw new ArgumentNullException(nameof(producer));
            }

            return producerFactories.GetOrAdd(producer).GetOffsetSource();
        }

        /// <summary>
        /// Represents a producer as a target block for a batch of Kafka key/value pairs.
        /// </summary>
        /// <remarks>
        /// All messages/offsets in a batch are produced atomically by the underlying client, using transactions.
        /// <para>
        /// A client batch includes a single batch from each associated target. If a client has multiple batch targets, they must produce the same number of batches (empty batches are permitted).
        /// </para>
        /// </remarks>
        /// <typeparam name="TKey">The producer key type.</typeparam>
        /// <typeparam name="TValue">The producer value type.</typeparam>
        /// <param name="producer">The producer.</param>
        /// <param name="topicPartition">The topic/partition receiving the messages. Use <see cref="Partition.Any"/> for automatic partitioning.</param>
        /// <param name="options">Options configuring batch producing.</param>
        /// <returns>The producer target block.</returns>
        public static ITargetBlock<IEnumerable<KeyValuePair<TKey, TValue>>> AsBatchTarget<TKey, TValue>(
            this IProducer<TKey, TValue> producer,
            TopicPartition topicPartition,
            BatchedProduceOptions? options = null)
        {
            if (producer == null)
            {
                throw new ArgumentNullException(nameof(producer));
            }

            if (topicPartition == null)
            {
                throw new ArgumentNullException(nameof(topicPartition));
            }

            return producerFactories.GetOrAdd(producer).GetBatchTarget(producer, topicPartition, options);
        }

        /// <summary>
        /// Represents a producer as a target block for a batch of Kafka offsets.
        /// </summary>
        /// <remarks>
        /// All messages/offsets in a batch are produced atomically by the underlying client, using transactions.
        /// <para>
        /// A client batch includes a single batch from each associated target. If a client has multiple batch targets, they must produce the same number of batches (empty batches are permitted).
        /// </para>
        /// </remarks>
        /// <typeparam name="TKey">The producer key type.</typeparam>
        /// <typeparam name="TValue">The producer value type.</typeparam>
        /// <param name="producer">The producer.</param>
        /// <param name="consumerGroup">The consumer group associated with the offsets. This can be retrieved from <see cref="IConsumer{TKey, TValue}.ConsumerGroupMetadata"/>.</param>
        /// <param name="options">Options configuring batch producing.</param>
        /// <returns>The offset target block.</returns>
        public static ITargetBlock<IEnumerable<TopicPartitionOffset>> AsOffsetTarget<TKey, TValue>(
            this IProducer<TKey, TValue> producer,
            IConsumerGroupMetadata consumerGroup,
            BatchedProduceOptions? options = null)
        {
            if (producer == null)
            {
                throw new ArgumentNullException(nameof(producer));
            }

            if (consumerGroup == null)
            {
                throw new ArgumentNullException(nameof(consumerGroup));
            }

            return producerFactories.GetOrAdd(producer).GetOffsetTarget(producer, consumerGroup, options);
        }

        /// <summary>
        /// Represents a consumer as a source block for Kafka key/value pairs.
        /// </summary>
        /// <remarks>
        /// Consumers must be subscribed/assigned in order to produce data.
        /// <para>
        /// To receive corresponding offsets, use <see cref="AsOffsetSource{TKey, TValue}(IConsumer{TKey, TValue})"/>.
        /// </para>
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <returns>The consumer source block.</returns>
        public static ISourceBlock<KeyValuePair<TKey, TValue>> AsSource<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer)
        {
            if (consumer == null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            return consumerFactories.GetOrAdd(consumer).GetSource(consumer);
        }

        /// <summary>
        /// Represents a consumer as a source block for consumed Kafka offsets.
        /// </summary>
        /// <remarks>
        /// To store processed offsets, use <see cref="AsOffsetTarget{TKey, TValue}(IConsumer{TKey, TValue})"/>.
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <returns>The offset source block.</returns>
        public static IReceivableSourceBlock<TopicPartitionOffset> AsOffsetSource<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer)
        {
            if (consumer == null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            return consumerFactories.GetOrAdd(consumer).GetOffsetSource();
        }

        /// <summary>
        /// Represents a consumer as a target block for processed Kafka offsets.
        /// </summary>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <returns>The offset target block.</returns>
        public static ITargetBlock<TopicPartitionOffset> AsOffsetTarget<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer)
        {
            if (consumer == null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            return ConsumerBlockFactory.GetOffsetTarget(consumer);
        }
    }
}
