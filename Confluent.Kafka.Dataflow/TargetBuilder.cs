namespace Confluent.Kafka.Dataflow
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading.Tasks.Dataflow;
    using Confluent.Kafka.Dataflow.Blocks;
    using Confluent.Kafka.Dataflow.Consuming;
    using Confluent.Kafka.Dataflow.Producing;

    /// <summary>
    /// A builder of <see cref="ITargetBlock{TInput}"/> instances to send data to Kafka transactionally (producing
    /// and/or committing consumed offsets) .
    /// </summary>
    /// <remarks>
    /// Use members to configure what data is sent for <typeparamref name="T"/>. All data mapped to a given instance
    /// will be included in the same transaction.
    /// </remarks>
    /// <typeparam name="T">The target data type.</typeparam>
    public class TargetBuilder<T>
    {
        readonly List<IMessageSender<T>> messageSenders = new();

        IClient? producerHandle;
        IProducerConsumerCollection<TopicPartitionOffset>? offsetSource;
        Func<T, int>? offsetMapping;
        IConsumerGroupMetadata? consumerMetadata;
        ITransactor<T>? commitTransactor;

        /// <summary>
        /// Configures transactional producing of Kafka messages.
        /// </summary>
        /// <remarks>
        /// Producer must be initialized with <see cref="IProducer{TKey, TValue}.InitTransactions"/>. Invoke multiple
        /// times to send to different destinations using the same underlying client.
        /// </remarks>
        /// <typeparam name="TKey">The producer key type.</typeparam>
        /// <typeparam name="TValue">The producer value type.</typeparam>
        /// <param name="producer">The target producer for these messages.</param>
        /// <param name="mapping">A delegate mapping <typeparamref name="T"/> to zero or more Kafka messages.</param>
        /// <param name="topicPartition">
        /// The topic/partition receiving the messages. Use <see cref="Partition.Any"/> for automatic partitioning.
        /// </param>
        /// <returns>The same instance for chaining.</returns>
        public TargetBuilder<T> WithMessages<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            Func<T, IEnumerable<Message<TKey, TValue>>> mapping,
            TopicPartition topicPartition)
        {
            if (producer == null)
            {
                throw new ArgumentNullException(nameof(producer));
            }

            if (this.producerHandle != null && this.producerHandle.Name != producer.Name)
            {
                throw new InvalidOperationException("Already configured with a different underlying client!");
            }

            var sender = new MessageSender<T, TKey, TValue>(
                producer,
                mapping ?? throw new ArgumentNullException(nameof(mapping)),
                topicPartition ?? throw new ArgumentNullException(nameof(topicPartition)));

            this.messageSenders.Add(sender);
            this.producerHandle = producer;

            return this;
        }

        /// <summary>
        /// Configures the target as a stream with a linked source.
        /// </summary>
        /// <remarks>
        /// Source offsets will be committed transactionally with produced messages. Consumer must have
        /// <c>enable.auto.commit</c> set to false.
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer to use as the source for Kafka messages.</param>
        /// <param name="source">The linked source instance.</param>
        /// <param name="handler">A delegate invoked for each source Kafka message (before offering to targets).</param>
        /// <param name="options">Block options for source consuming.</param>
        /// <param name="mapping">
        /// A delegate mapping <typeparamref name="T"/> to a number of source offsets (one-to-one if
        /// <see langword="null"/>).
        /// </param>
        /// <returns>The same instance for chaining.</returns>
        [Obsolete(
            "Message order need not be preserved in all cases (multi-partition scenarios). " +
            "Use overload with explict source message mapping.")]
        public TargetBuilder<T> AsStream<TKey, TValue>(
            IConsumer<TKey, TValue> consumer,
            out ISourceBlock<Message<TKey, TValue>> source,
            Action<Message<TKey, TValue>, TopicPartitionOffset>? handler = null,
            DataflowBlockOptions? options = null,
            Func<T, int>? mapping = null)
        {
            if (this.consumerMetadata != null)
            {
                throw new InvalidOperationException("Already configured as a stream!");
            }

            var loader = new MessageLoader<TKey, TValue>(consumer ?? throw new ArgumentNullException(nameof(consumer)));
            loader.OnConsumed += handler;

            var offsets = new ConcurrentQueue<TopicPartitionOffset>();
            loader.OnConsumed += (_, tpo) => offsets.Enqueue(tpo);
            this.offsetSource = offsets;

            source = new CustomBlock<Message<TKey, TValue>>(loader.Load, options ?? new());

            this.offsetMapping = mapping;
            this.consumerMetadata = consumer.ConsumerGroupMetadata;
            this.commitTransactor = new OffsetTransactor<T, TKey, TValue>(consumer);

            return this;
        }

        /// <summary>
        /// Configures the target as a stream with a linked source.
        /// </summary>
        /// <remarks>
        /// Source offsets will be committed transactionally with produced messages. Consumer must have
        /// <c>enable.auto.commit</c> set to false.
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer to use as the source for Kafka messages.</param>
        /// <param name="source">The linked source instance.</param>
        /// <param name="mapping">
        /// A delegate mapping <typeparamref name="T"/> to zero or more source Kafka messages.
        /// </param>
        /// <param name="handler">A delegate invoked for each source Kafka message (before offering to targets).</param>
        /// <param name="options">Block options for source consuming.</param>
        /// <returns>The same instance for chaining.</returns>
        public TargetBuilder<T> AsStream<TKey, TValue>(
            IConsumer<TKey, TValue> consumer,
            out ISourceBlock<Message<TKey, TValue>> source,
            Func<T, IEnumerable<Message<TKey, TValue>>> mapping,
            Action<Message<TKey, TValue>, TopicPartitionOffset>? handler = null,
            DataflowBlockOptions? options = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Builds the configured target block.
        /// </summary>
        /// <param name="handler">
        /// A delegate invoked for each item delivered to Kafka, with its corresponding offests.
        /// </param>
        /// <param name="options">Block options for sending data.</param>
        /// <param name="interval">The interval between transactions (5 seconds if <see langword="null"/>).</param>
        /// <returns>The built target instance.</returns>
        public ITargetBlock<T> Build(
            Action<T, IReadOnlyList<TopicPartitionOffset>>? handler = null,
            DataflowBlockOptions? options = null,
            TimeSpan? interval = null)
        {
            ITransactor<T> transactor;

            if (this.producerHandle != null)
            {
                var producer = new DependentProducerBuilder<Null, Null>(this.producerHandle.Handle).Build();
                transactor = new MessageTransactor<T>(producer, this.messageSenders, this.consumerMetadata);
            }
            else
            {
                transactor = this.commitTransactor ?? throw new InvalidOperationException("No data mapped to T!");
            }

            var flusher = new TransactionFlusher<T>(
                transactor,
                interval ?? TimeSpan.FromSeconds(5),
                this.offsetSource,
                this.offsetMapping);

            flusher.OnDelivered += handler;

            return new CustomBlock<T>(flusher.Flush, options ?? new());
        }
    }
}
