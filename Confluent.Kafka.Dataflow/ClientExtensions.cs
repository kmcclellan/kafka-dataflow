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
            return GetBlock(consumer, f => f.GetSource());
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
        public static ISourceBlock<TopicPartitionOffset> AsOffsetSource<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer)
        {
            return GetBlock(consumer, f => f.GetOffsetSource());
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
            return GetBlock(consumer, f => f.GetOffsetTarget());
        }

        private static TBlock GetBlock<TKey, TValue, TBlock>(
            IConsumer<TKey, TValue> consumer,
            Func<ConsumerBlockFactory<TKey, TValue>, TBlock> factory)
        {
            return GetBlock(
                consumer,
                (IConsumer<TKey, TValue> c, ClientState<ITargetBlock<TopicPartitionOffset>> s) =>
                    new ConsumerBlockFactory<TKey, TValue>(c, s),
                factory);
        }

        private static TBlock GetBlock<TClient, TState, TFactory, TBlock>(
            TClient client,
            Func<TClient, ClientState<TState>, TFactory> getFactory,
            Func<TFactory, TBlock> getBlock)
            where TClient : IClient
        {
            var clientState = ClientState<TState>.Get(client);

            // Client state is stored statically, so make sure this is thread-safe.
            lock (clientState)
            {
                return getBlock(getFactory(client, clientState));
            }
        }
    }
}
