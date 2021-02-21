namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ConsumerBlockFactory<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> consumer;

        public ConsumerBlockFactory(IConsumer<TKey, TValue> consumer)
        {
            this.consumer = consumer;
        }

        public ISourceBlock<KeyValuePair<TKey, TValue>> GetSource()
        {
            var buffer = new BufferBlock<KeyValuePair<TKey, TValue>>(new DataflowBlockOptions
            {
                // Consumers do their own buffering.
                // We just want a block to hold the current message.
                BoundedCapacity = 1,
            });

            var completionSource = new TaskCompletionSource<byte>();
            return new CustomSource<KeyValuePair<TKey, TValue>>(
                buffer,
                completionSource,
                () => this.StartConsuming(buffer, completionSource.Task));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Dataflow fault")]
        async void StartConsuming(ITargetBlock<KeyValuePair<TKey, TValue>> target, Task completion)
        {
            try
            {
                // Continue on thread pool.
                await Task.Yield();
                while (!completion.IsCompleted)
                {
                    var result = this.consumer.Consume(100);
                    if (result != null)
                    {
                        var kvp = new KeyValuePair<TKey, TValue>(result.Message.Key, result.Message.Value);
                        if (!target.Post(kvp))
                        {
                            await target.SendAsync(kvp);
                        }
                    }
                }

                // Observe any exceptions.
                await completion;
            }
            catch (Exception exception)
            {
                target.Fault(exception);
            }

            target.Complete();
        }
    }
}
