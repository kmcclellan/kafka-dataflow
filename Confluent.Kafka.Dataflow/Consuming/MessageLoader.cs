namespace Confluent.Kafka.Dataflow.Consuming
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class MessageLoader<TKey, TValue>
    {
        readonly IConsumer<TKey, TValue> consumer;

        public MessageLoader(IConsumer<TKey, TValue> consumer)
        {
            this.consumer = consumer;
        }

        public event Action<Message<TKey, TValue>, TopicPartitionOffset>? OnConsumed;

        public async Task Load(ITargetBlock<Message<TKey, TValue>> target, CancellationToken cancellationToken)
        {
            ConsumeResult<TKey, TValue> result;

            while (true)
            {
                result = this.consumer.Consume(TimeSpan.Zero) ??
                    await this.ConsumeAsync(cancellationToken).ConfigureAwait(false);

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                this.OnConsumed?.Invoke(result.Message, result.TopicPartitionOffset);

                if (!target.Post(result.Message) &&
                    !await target.SendAsync(result.Message, CancellationToken.None).ConfigureAwait(false))
                {
                    throw new InvalidOperationException("Target rejected message!");
                }
            }
        }

        private Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken)
        {
            return Task.Factory.StartNew(
                obj =>
                {
                    var (c, ct) = ((IConsumer<TKey, TValue>, CancellationToken))obj!;
                    return c.Consume(ct);
                },
                (this.consumer, cancellationToken),
                cancellationToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Current);
        }
    }
}
