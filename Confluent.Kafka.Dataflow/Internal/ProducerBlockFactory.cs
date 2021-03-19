namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ProducerBlockFactory
    {
        readonly TaskCompletionSource<byte> completionSource = new();
        ITargetBlock<IReadOnlyList<TopicPartitionOffset>>? offsetTarget;
        readonly List<Func<int, ICollection<TopicPartitionOffset>, Task>> batchHandlers = new();

        public ITargetBlock<KeyValuePair<TKey, TValue>> GetTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            TopicPartition topicPartition)
        {
            var processor = new ActionBlock<KeyValuePair<TKey, TValue>>(
                kvp => producer.Produce(
                    topicPartition,
                    new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value },
                    x => offsetTarget?.Post(new[] { x.TopicPartitionOffset })));

            return new CustomTarget<KeyValuePair<TKey, TValue>>(
                processor,
                ContinueWithFlush(producer, processor.Completion));
        }

        public IReceivableSourceBlock<IReadOnlyList<TopicPartitionOffset>> GetOffsetSource()
        {
            var buffer = new BufferBlock<IReadOnlyList<TopicPartitionOffset>>();
            this.offsetTarget = buffer;
            this.completionSource.Task.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    this.offsetTarget.Fault(t.Exception!);
                }
                else
                {
                    this.offsetTarget.Complete();
                }
            }, TaskScheduler.Default);

            return buffer;
        }

        public ITargetBlock<IEnumerable<KeyValuePair<TKey, TValue>>> GetBatchTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            TopicPartition topicPartition,
            BatchedProduceOptions? options)
        {
            return this.GetBatchTarget<TKey, TValue, KeyValuePair<TKey, TValue>>(
                producer,
                async (messages, offsets) =>
                {
                    var tasks = new List<Task<DeliveryResult<TKey, TValue>>>();

                    foreach (var kvp in messages)
                    {
                        var message = new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value };
                        tasks.Add(producer.ProduceAsync(topicPartition, message));
                    }

                    foreach (var result in await Task.WhenAll(tasks))
                    {
                        offsets.Add(result.TopicPartitionOffset);
                    }
                },
                options);
        }

        public ITargetBlock<IEnumerable<TopicPartitionOffset>> GetOffsetTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            IConsumerGroupMetadata consumerGroup,
            BatchedProduceOptions? options)
        {
            return this.GetBatchTarget<TKey, TValue, TopicPartitionOffset>(
                producer,
                async (inOffsets, outOffsets) =>
                {
                    var commitOffsets = new Dictionary<TopicPartition, TopicPartitionOffset>();
                    foreach (var offset in inOffsets)
                    {
                        outOffsets.Add(offset);

                        // Track the latest offset per partition.
                        // (Client seems to unable to handle a large number of offsets).
                        commitOffsets[offset.TopicPartition] = 
                            new TopicPartitionOffset(offset.TopicPartition, offset.Offset + 1);
                    }

                    await Task.Yield();
                    producer.SendOffsetsToTransaction(commitOffsets.Values, consumerGroup, Timeout.InfiniteTimeSpan);
                },
                options);
        }

        ITargetBlock<IEnumerable<TItem>> GetBatchTarget<TKey, TValue, TItem>(
            IProducer<TKey, TValue> producer,
            Func<IEnumerable<TItem>, ICollection<TopicPartitionOffset>, Task> batchHandler,
            BatchedProduceOptions? options)
        {
            options ??= new BatchedProduceOptions();

            var buffer = new BufferBlock<IEnumerable<TItem>>(new DataflowBlockOptions
            {
                BoundedCapacity = options.BoundedCapacity ?? DataflowBlockOptions.Unbounded,
            });

            if (this.batchHandlers.Count == 0)
            {
                this.StartTransactions(producer, buffer, options.TransactionInterval);
            }

            batchHandlers.Add(async (batchCount, offsets) =>
            {
                var items = new List<TItem>();
                for (var i = 0; i < batchCount; i++)
                {
                    if (!buffer.TryReceive(out var batch))
                    {
                        batch = await buffer.ReceiveAsync();
                    }

                    items.AddRange(batch);
                }

                await batchHandler(items, offsets);
            });

            return new CustomTarget<IEnumerable<TItem>>(
                buffer,
                this.completionSource.Task);
        }

        static async Task ContinueWithFlush<TKey, TValue>(IProducer<TKey, TValue> producer, Task completion)
        {
            await completion;
            producer.Flush(Timeout.InfiniteTimeSpan);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Dataflow fault")]
        async void StartTransactions<TKey, TValue, TItem>(
            IProducer<TKey, TValue> producer,
            BufferBlock<IEnumerable<TItem>> buffer,
            TimeSpan interval)
        {
            try
            {
                await Task.Yield();
                producer.InitTransactions(Timeout.InfiniteTimeSpan);

                bool completed;
                do
                {
                    // Run one more time after completion.
                    completed = buffer.Completion.IsCompleted;
                    var nextTransactionTime = DateTime.Now + interval;
                    var batches = buffer.Count;

                    if (batches > 0 && this.batchHandlers.Count > 0)
                    {
                        var offsets = new List<TopicPartitionOffset>();

                        producer.BeginTransaction();

                        foreach (var handler in this.batchHandlers)
                        {
                            // Tell the handler how many batches to read.
                            // (All targets should produce the same number of batches.)
                            await handler(batches, offsets);
                        }

                        producer.CommitTransaction(Timeout.InfiniteTimeSpan);
                        this.offsetTarget?.Post(offsets);
                    }

                    var delay = nextTransactionTime - DateTime.Now;
                    if (delay > TimeSpan.Zero)
                    {
                        await Task.WhenAny(Task.Delay(delay), buffer.Completion);
                    }
                }
                while (!completed);

                // Observe any exceptions.
                await buffer.Completion;
            }
            catch (Exception exception)
            {
                this.completionSource.TrySetException(exception);
            }

            this.completionSource.TrySetResult(default);
        }
    }
}
