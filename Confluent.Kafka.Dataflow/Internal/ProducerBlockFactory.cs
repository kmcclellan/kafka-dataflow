namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ProducerBlockFactory
    {
        readonly Lazy<Task> batchingTask;
        readonly List<IBlockProducer> batchProducers = new();

        BufferBlock<IReadOnlyList<TopicPartitionOffset>>? deliveredOffsets;

        public ProducerBlockFactory()
        {
            this.batchingTask = new Lazy<Task>(this.TransactionLoop, LazyThreadSafetyMode.ExecutionAndPublication);
        }

        public ITargetBlock<KeyValuePair<TKey, TValue>> GetTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            TopicPartition topicPartition)
        {
            var processor = new ActionBlock<KeyValuePair<TKey, TValue>>(
                kvp => producer.Produce(
                    topicPartition,
                    new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value },
                    x => deliveredOffsets?.Post(new[] { x.TopicPartitionOffset })));

            return processor.ContinueWith(
                new Lazy<Task>(async () =>
                {
                    await Task.Yield();
                    producer.Flush();
                }));
        }

        public IReceivableSourceBlock<IReadOnlyList<TopicPartitionOffset>> GetOffsetSource()
        {
            this.deliveredOffsets ??= new();
            return this.deliveredOffsets;
        }

        public ITargetBlock<IEnumerable<KeyValuePair<TKey, TValue>>> GetBatchTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            TopicPartition topicPartition,
            BatchedProduceOptions? options)
        {
            var buffer = new MessageProducer<TKey, TValue>(producer, topicPartition, options);
            this.batchProducers.Add(buffer);
            return buffer.Target.BeginWith(this.batchingTask);
        }

        public ITargetBlock<IEnumerable<TopicPartitionOffset>> GetOffsetTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            IConsumerGroupMetadata consumerGroup,
            BatchedProduceOptions? options)
        {
            var buffer = new OffsetProducer<TKey, TValue>(producer, consumerGroup, options);
            this.batchProducers.Add(buffer);
            return buffer.Target.BeginWith(this.batchingTask);
        }

        async Task TransactionLoop()
        {
            try
            {
                var leader = this.batchProducers.FirstOrDefault() ?? throw new InvalidOperationException("No batch targets.");
                var timestamp = default(DateTime?);

                int? batches;
                while ((batches = await leader.Prepare(timestamp)) != null)
                {
                    if (batches > 0)
                    {
                        var offsets = new List<TopicPartitionOffset>();

                        for (var i = 0; i < this.batchProducers.Count; i++)
                        {
                            // All targets should produce the same number of batches.
                            await this.batchProducers[i].Produce(batches.Value, offsets, i == this.batchProducers.Count - 1);
                        }

                        this.deliveredOffsets?.Post(offsets);
                    }

                    timestamp = DateTime.UtcNow;
                }

                this.deliveredOffsets?.Complete();
            }
            catch (Exception exception)
            {
                (this.deliveredOffsets as IDataflowBlock)?.Fault(exception);
                throw;
            }
        }

        interface IBlockProducer
        {
            Task<int?> Prepare(DateTime? previous);

            Task Produce(int batches, ICollection<TopicPartitionOffset> offsets, bool commit);
        }

        abstract class BlockProducer<TKey, TValue, TItem> : IBlockProducer
        {
            readonly IProducer<TKey, TValue> producer;
            readonly BatchedProduceOptions options;
            readonly BufferBlock<IEnumerable<TItem>> buffer;

            public BlockProducer(IProducer<TKey, TValue> producer, BatchedProduceOptions? options)
            {
                this.producer = producer;
                this.options = options ?? new BatchedProduceOptions();

                this.buffer = new(new DataflowBlockOptions
                {
                    BoundedCapacity = this.options.BoundedCapacity ?? DataflowBlockOptions.Unbounded,
                });
            }

            public ITargetBlock<IEnumerable<TItem>> Target => this.buffer;

            public async Task<int?> Prepare(DateTime? previous)
            {
                var delay = (previous ?? DateTime.MinValue) + this.options.TransactionInterval - DateTime.UtcNow;
                if (delay > TimeSpan.Zero)
                {
                    await await Task.WhenAny(Task.Delay(delay), this.buffer.Completion);
                }

                if (this.buffer.Completion.IsCompleted)
                {
                    return null;
                }

                var batches = this.buffer.Count;
                if (batches > 0)
                {
                    this.producer.BeginTransaction();
                }

                return batches;
            }

            public async Task Produce(int batches, ICollection<TopicPartitionOffset> offsets, bool commit)
            {
                var items = new List<TItem>();

                for (var i = 0; i < batches; i++)
                {
                    if (!this.buffer.TryReceive(out var batch))
                    {
                        batch = await this.buffer.ReceiveAsync();
                    }

                    items.AddRange(batch);
                }

                await this.Produce(items, offsets);

                if (commit)
                {
                    this.producer.CommitTransaction();
                }
            }

            protected abstract Task Produce(IReadOnlyList<TItem> items, ICollection<TopicPartitionOffset> offsets);
        }

        class MessageProducer<TKey, TValue> : BlockProducer<TKey, TValue, KeyValuePair<TKey, TValue>>
        {
            readonly IProducer<TKey, TValue> producer;
            readonly TopicPartition topicPartition;

            public MessageProducer(IProducer<TKey, TValue> producer, TopicPartition topicPartition, BatchedProduceOptions? options)
                : base(producer, options)
            {
                this.producer = producer;
                this.topicPartition = topicPartition;
            }

            protected override async Task Produce(
                IReadOnlyList<KeyValuePair<TKey, TValue>> items,
                ICollection<TopicPartitionOffset> offsets)
            {
                var tasks = new List<Task<DeliveryResult<TKey, TValue>>>();

                foreach (var message in items)
                {
                    var task = this.producer.ProduceAsync(
                        topicPartition,
                        new Message<TKey, TValue> { Key = message.Key, Value = message.Value });

                    tasks.Add(task);
                }

                foreach (var result in await Task.WhenAll(tasks))
                {
                    offsets.Add(result.TopicPartitionOffset);
                }
            }
        }

        class OffsetProducer<TKey, TValue> : BlockProducer<TKey, TValue, TopicPartitionOffset>
        {
            readonly IProducer<TKey, TValue> producer;
            readonly IConsumerGroupMetadata consumerGroup;

            public OffsetProducer(IProducer<TKey, TValue> producer, IConsumerGroupMetadata consumerGroup, BatchedProduceOptions? options)
                : base(producer, options)
            {
                this.producer = producer;
                this.consumerGroup = consumerGroup;
            }

            protected override async Task Produce(IReadOnlyList<TopicPartitionOffset> items, ICollection<TopicPartitionOffset> offsets)
            {
                var commitOffsets = new Dictionary<TopicPartition, TopicPartitionOffset>();
                foreach (var offset in items)
                {
                    offsets.Add(offset);

                    // Track the latest offset per partition.
                    // (Client seems to unable to handle a large number of offsets).
                    commitOffsets[offset.TopicPartition] =
                        new TopicPartitionOffset(offset.TopicPartition, offset.Offset + 1);
                }

                await Task.Yield();
                producer.SendOffsetsToTransaction(commitOffsets.Values, consumerGroup, Timeout.InfiniteTimeSpan);
            }
        }
    }
}
