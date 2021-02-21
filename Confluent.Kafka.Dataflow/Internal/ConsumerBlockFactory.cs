namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ConsumerBlockFactory<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> consumer;
        private readonly ClientState<ITargetBlock<TopicPartitionOffset>> clientState;

        public ConsumerBlockFactory(
            IConsumer<TKey, TValue> consumer,
            ClientState<ITargetBlock<TopicPartitionOffset>> clientState)
        {
            this.consumer = consumer;
            this.clientState = clientState;
        }

        public ISourceBlock<KeyValuePair<TKey, TValue>> GetSource()
        {
            var buffer = new BufferBlock<KeyValuePair<TKey, TValue>>(new DataflowBlockOptions
            {
                // Consumers do their own buffering.
                // We just want a block to hold the current message.
                BoundedCapacity = 1,
            });

            return new CustomSource<KeyValuePair<TKey, TValue>>(
                buffer,
                this.clientState.CompletionSource,
                () => this.StartConsuming(buffer));
        }

        public ISourceBlock<TopicPartitionOffset> GetOffsetSource()
        {
            var buffer = new BufferBlock<TopicPartitionOffset>();
            this.clientState.State = buffer;
            return new CustomSource<TopicPartitionOffset>(buffer, this.clientState.CompletionSource);
        }

        public ITargetBlock<TopicPartitionOffset> GetOffsetTarget()
        {
            var processor = new ActionBlock<TopicPartitionOffset>(
                x => this.consumer.StoreOffset(new TopicPartitionOffset(x.TopicPartition, x.Offset + 1)));

            return new CustomTarget<TopicPartitionOffset>(
                processor,
                this.ContinueWithCommit(processor.Completion));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Dataflow fault")]
        async void StartConsuming(ITargetBlock<KeyValuePair<TKey, TValue>> target)
        {
            try
            {
                // Continue on thread pool.
                await Task.Yield();
                while (!this.clientState.CompletionSource.Task.IsCompleted)
                {
                    var result = this.consumer.Consume(100);
                    if (result != null)
                    {
                        var kvp = new KeyValuePair<TKey, TValue>(result.Message.Key, result.Message.Value);
                        if (!target.Post(kvp))
                        {
                            await target.SendAsync(kvp);
                        }

                        // Target should never postpone (unbounded).
                        this.clientState.State?.Post(result.TopicPartitionOffset);
                    }
                }

                // Observe any exceptions.
                await this.clientState.CompletionSource.Task;
            }
            catch (Exception exception)
            {
                target.Fault(exception);
                this.clientState.State?.Fault(exception);
            }

            target.Complete();
            this.clientState.State?.Complete();
        }

        async Task ContinueWithCommit(Task completion)
        {
            await completion;
            this.consumer.Commit();
        }
    }
}
