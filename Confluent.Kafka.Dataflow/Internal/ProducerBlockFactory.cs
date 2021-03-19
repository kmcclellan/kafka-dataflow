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
        Func<int, Task>? batchHandler;

        public static ITargetBlock<KeyValuePair<TKey, TValue>> GetTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            TopicPartition topicPartition)
        {
            // No delivery handler support at this time. (Implement as a separate source block.)
            var processor = new ActionBlock<KeyValuePair<TKey, TValue>>(
                kvp => producer.Produce(
                    topicPartition,
                    new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value }));

            return new CustomTarget<KeyValuePair<TKey, TValue>>(
                processor,
                ContinueWithFlush(producer, processor.Completion));
        }

        public ITargetBlock<IEnumerable<KeyValuePair<TKey, TValue>>> GetBatchTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            TopicPartition topicPartition,
            BatchedProduceOptions? options)
        {
            return this.GetBatchTarget(
                producer,
                (IEnumerable<KeyValuePair<TKey, TValue>> messages) =>
                {
                    foreach (var kvp in messages)
                    {
                        producer.Produce(
                            topicPartition,
                            new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value });
                    }
                },
                options);
        }

        public ITargetBlock<IEnumerable<TopicPartitionOffset>> GetOffsetTarget<TKey, TValue>(
            IProducer<TKey, TValue> producer,
            IConsumerGroupMetadata consumerGroup,
            BatchedProduceOptions? options)
        {
            return this.GetBatchTarget(
                producer,
                (IEnumerable<TopicPartitionOffset> offsets) =>
                {
                    // Track the latest offset per partition.
                    // (Client seems to unable to handle a large number of offsets).
                    var commitOffsets = new Dictionary<TopicPartition, TopicPartitionOffset>();
                    foreach (var offset in offsets)
                    {
                        commitOffsets[offset.TopicPartition] = 
                            new TopicPartitionOffset(offset.TopicPartition, offset.Offset + 1);
                    }

                    producer.SendOffsetsToTransaction(commitOffsets.Values, consumerGroup, Timeout.InfiniteTimeSpan);
                },
                options);
        }

        ITargetBlock<IEnumerable<TItem>> GetBatchTarget<TKey, TValue, TItem>(
            IProducer<TKey, TValue> producer,
            Action<IEnumerable<TItem>> batchHandler,
            BatchedProduceOptions? options)
        {
            options ??= new BatchedProduceOptions();

            var buffer = new BufferBlock<IEnumerable<TItem>>(new DataflowBlockOptions
            {
                BoundedCapacity = options.BoundedCapacity ?? DataflowBlockOptions.Unbounded,
            });

            if (this.batchHandler == null)
            {
                this.StartTransactions(producer, buffer, options.TransactionInterval);
            }

            // Chain the handlers to coordinate multiple targets for the client.
            // (All targets should produce the same number of batches.)
            var prevHandler = this.batchHandler;
            this.batchHandler = async batchCount =>
            {
                if (prevHandler != null)
                {
                    await prevHandler(batchCount);
                }

                var items = new List<TItem>();
                for (var i = 0; i < batchCount; i++)
                {
                    if (!buffer.TryReceive(out var batch))
                    {
                        batch = await buffer.ReceiveAsync();
                    }

                    items.AddRange(batch);
                }

                batchHandler(items);
            };

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

                    if (buffer.Count > 0 && this.batchHandler != null)
                    {
                        producer.BeginTransaction();

                        // There may be multiple parallel batch handlers here.
                        // Just tell them how many batches to read.
                        await this.batchHandler(buffer.Count);
                        producer.CommitTransaction(Timeout.InfiniteTimeSpan);
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
