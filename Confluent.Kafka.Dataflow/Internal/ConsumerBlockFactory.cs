namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ConsumerBlockFactory
    {
        readonly Lazy<Task> consumingTask;

        IBlockConsumer? consumer;
        BufferBlock<TopicPartitionOffset>? consumedOffsets;

        public ConsumerBlockFactory()
        {
            this.consumingTask = new Lazy<Task>(this.ConsumeLoop, LazyThreadSafetyMode.ExecutionAndPublication);
        }

        public IReceivableSourceBlock<KeyValuePair<TKey, TValue>> GetSource<TKey, TValue>(IConsumer<TKey, TValue> consumer)
        {
            if (this.consumingTask.IsValueCreated || this.consumer != null)
            {
                throw new InvalidOperationException("Consumer is already in use.");
            }

            var buffer = new BlockConsumer<TKey, TValue>(consumer);
            this.consumer = buffer;
            return buffer.Source.BeginWith(this.consumingTask);
        }

        public IReceivableSourceBlock<TopicPartitionOffset> GetOffsetSource()
        {
            if (this.consumingTask.IsValueCreated)
            {
                throw new InvalidOperationException("Consumer is already in use.");
            }

            this.consumedOffsets ??= new();
            return this.consumedOffsets;
        }

        public static ITargetBlock<TopicPartitionOffset> GetOffsetTarget<TKey, TValue>(IConsumer<TKey, TValue> consumer)
        {
            var processor = new ActionBlock<TopicPartitionOffset>(
                x => consumer.StoreOffset(new TopicPartitionOffset(x.TopicPartition, x.Offset + 1)));

            return processor.ContinueWith(
                new Lazy<Task>(async () =>
                {
                    await Task.Yield();
                    consumer.Commit();
                }));
        }

        async Task ConsumeLoop()
        {
            try
            {
                if (this.consumer == null)
                {
                    throw new InvalidOperationException("No message source.");
                }

                using var cancellation = this.consumer.Buffer.GetCompletionToken();
                while (!cancellation.IsCancellationRequested)
                {
                    try
                    {
                        var offset = await this.consumer.Consume(cancellation.Token);
                        this.consumedOffsets?.Post(offset);
                    }
                    catch (OperationCanceledException)
                    {
                        // Caller did not request cancellation, so swallow this.
                    }
                }

                this.consumedOffsets?.Complete();
            }
            catch (Exception exception)
            {
                (this.consumedOffsets as IDataflowBlock)?.Fault(exception);
                throw;
            }
        }

        interface IBlockConsumer
        {
            IDataflowBlock Buffer { get; }

            Task<TopicPartitionOffset> Consume(CancellationToken cancellationToken);
        }

        class BlockConsumer<TKey, TValue> : IBlockConsumer
        {
            static readonly DataflowBlockOptions BufferOptions = new()
            {
                // Consumers do their own buffering.
                // We just want a block to hold the current message.
                BoundedCapacity = 1,
            };

            readonly IConsumer<TKey, TValue> consumer;
            readonly BufferBlock<KeyValuePair<TKey, TValue>> buffer = new(BufferOptions);

            public BlockConsumer(IConsumer<TKey, TValue> consumer)
            {
                this.consumer = consumer;
            }

            public IDataflowBlock Buffer => this.buffer;

            public IReceivableSourceBlock<KeyValuePair<TKey, TValue>> Source => this.buffer;

            public async Task<TopicPartitionOffset> Consume(CancellationToken cancellationToken)
            {
                await Task.Yield();
                var result = this.consumer.Consume(cancellationToken);
                var kvp = new KeyValuePair<TKey, TValue>(result.Message.Key, result.Message.Value);

                if (!this.buffer.Post(kvp))
                {
                    // Match librdkafka default.
                    const int HEARTBEAT_INTERVAL = 3000;

                    var task = this.buffer.SendAsync(kvp, cancellationToken);
                    while (await Task.WhenAny(task, Task.Delay(HEARTBEAT_INTERVAL, cancellationToken)) != task)
                    {
                        // Reconsume to stay in consumer group.
                        this.consumer.Seek(result.TopicPartitionOffset);
                        this.consumer.Consume(cancellationToken);
                    }

                    if (!task.Result)
                    {
                        await this.buffer.Completion;

                        // Buffer rejected message (don't send offset).
                        throw new OperationCanceledException();
                    }
                }

                return result.TopicPartitionOffset;
            }
        }
    }
}
