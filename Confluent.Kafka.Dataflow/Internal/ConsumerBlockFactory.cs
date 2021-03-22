namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ConsumerBlockFactory
    {
        readonly TaskCompletionSource<byte> completionSource = new();
        ITargetBlock<TopicPartitionOffset>? offsetTarget;

        public ISourceBlock<KeyValuePair<TKey, TValue>> GetSource<TKey, TValue>(IConsumer<TKey, TValue> consumer)
        {
            var buffer = new BufferBlock<KeyValuePair<TKey, TValue>>(new DataflowBlockOptions
            {
                // Consumers do their own buffering.
                // We just want a block to hold the current message.
                BoundedCapacity = 1,
            });

            return new CustomSource<KeyValuePair<TKey, TValue>>(
                buffer,
                this.completionSource,
                () => this.StartConsuming(consumer, buffer));
        }

        public IReceivableSourceBlock<TopicPartitionOffset> GetOffsetSource()
        {
            var buffer = new BufferBlock<TopicPartitionOffset>();
            this.offsetTarget = buffer;
            return new CustomSource<TopicPartitionOffset>(buffer, this.completionSource);
        }

        public static ITargetBlock<TopicPartitionOffset> GetOffsetTarget<TKey, TValue>(IConsumer<TKey, TValue> consumer)
        {
            var processor = new ActionBlock<TopicPartitionOffset>(
                x => consumer.StoreOffset(new TopicPartitionOffset(x.TopicPartition, x.Offset + 1)));

            return new CustomTarget<TopicPartitionOffset>(
                processor,
                ContinueWithCommit(consumer, processor.Completion));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Dataflow fault")]
        async void StartConsuming<TKey, TValue>(
            IConsumer<TKey, TValue> consumer,
            ITargetBlock<KeyValuePair<TKey, TValue>> target)
        {
            try
            {
                // Continue on thread pool.
                await Task.Yield();
                while (!this.completionSource.Task.IsCompleted)
                {
                    var result = consumer.Consume(100);
                    if (result != null)
                    {
                        var kvp = new KeyValuePair<TKey, TValue>(result.Message.Key, result.Message.Value);
                        if (!target.Post(kvp))
                        {
                            await target.SendAsync(kvp);
                        }

                        // Target should never postpone (unbounded).
                        this.offsetTarget?.Post(result.TopicPartitionOffset);
                    }
                }

                // Observe any exceptions.
                await this.completionSource.Task;
            }
            catch (Exception exception)
            {
                target.Fault(exception);
                this.offsetTarget?.Fault(exception);
            }

            target.Complete();
            this.offsetTarget?.Complete();
        }

        static async Task ContinueWithCommit<TKey, TValue>(IConsumer<TKey, TValue> consumer, Task completion)
        {
            await completion;
            consumer.Commit();
        }
    }
}
