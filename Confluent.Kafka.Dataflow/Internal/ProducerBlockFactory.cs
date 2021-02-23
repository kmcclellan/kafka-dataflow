namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ProducerBlockFactory<TKey, TValue>
    {
        private readonly IProducer<TKey, TValue> producer;
        private readonly ClientState<Func<int, Task>> clientState;

        public ProducerBlockFactory(
            IProducer<TKey, TValue> producer,
            ClientState<Func<int, Task>> clientState)
        {
            this.producer = producer;
            this.clientState = clientState;
        }

        public ITargetBlock<KeyValuePair<TKey, TValue>> GetTarget(TopicPartition topicPartition)
        {
            // No delivery handler support at this time. (Implement as a separate source block.)
            var processor = new ActionBlock<KeyValuePair<TKey, TValue>>(
                kvp => this.producer.Produce(
                    topicPartition,
                    new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value }));

            return new CustomTarget<KeyValuePair<TKey, TValue>>(
                processor,
                this.ContinueWithFlush(processor.Completion));
        }

        public ITargetBlock<IEnumerable<KeyValuePair<TKey, TValue>>> GetBatchTarget(
            TopicPartition topicPartition,
            BatchedProduceOptions? options)
        {
            return this.GetBatchTarget<KeyValuePair<TKey, TValue>>(
                messages =>
                {
                    foreach (var kvp in messages)
                    {
                        this.producer.Produce(
                            topicPartition,
                            new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value });
                    }
                },
                options);
        }

        public ITargetBlock<IEnumerable<TopicPartitionOffset>> GetOffsetTarget(
            IConsumerGroupMetadata consumerGroup,
            BatchedProduceOptions? options)
        {
            return this.GetBatchTarget<TopicPartitionOffset>(
                offsets =>
                {
                    var commitOffsets = new List<TopicPartitionOffset>();
                    foreach (var offset in offsets)
                    {
                        commitOffsets.Add(new TopicPartitionOffset(offset.TopicPartition, offset.Offset + 1));
                    }

                    this.producer.SendOffsetsToTransaction(commitOffsets, consumerGroup, Timeout.InfiniteTimeSpan);
                },
                options);
        }

        private ITargetBlock<IEnumerable<T>> GetBatchTarget<T>(
            Action<IEnumerable<T>> batchHandler,
            BatchedProduceOptions? options)
        {
            options ??= new BatchedProduceOptions();

            var buffer = new BufferBlock<IEnumerable<T>>(new DataflowBlockOptions
            {
                BoundedCapacity = options.BoundedCapacity ?? DataflowBlockOptions.Unbounded,
            });

            if (this.clientState.State == null)
            {
                this.StartTransactions(producer, buffer, options.TransactionInterval);
            }

            // Chain the handlers to coordinate multiple targets for the client.
            // (All targets should produce the same number of batches.)
            var prevHandler = this.clientState.State;
            this.clientState.State = async batchCount =>
            {
                if (prevHandler != null)
                {
                    await prevHandler(batchCount);
                }

                var items = new List<T>();
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

            return new CustomTarget<IEnumerable<T>>(
                buffer,
                this.clientState.CompletionSource.Task);
        }

        async Task ContinueWithFlush(Task completion)
        {
            await completion;
            this.producer.Flush(Timeout.InfiniteTimeSpan);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Dataflow fault")]
        private async void StartTransactions<T>(
            IProducer<TKey, TValue> producer,
            BufferBlock<T> buffer,
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

                    if (buffer.Count > 0 && this.clientState.State != null)
                    {
                        producer.BeginTransaction();

                        // There may be multiple parallel batch handlers here.
                        // Just tell them how many batches to read.
                        await this.clientState.State(buffer.Count);
                        this.producer.CommitTransaction(Timeout.InfiniteTimeSpan);
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
                this.clientState.CompletionSource.TrySetException(exception);
            }

            this.clientState.CompletionSource.TrySetResult(default);
        }
    }
}
