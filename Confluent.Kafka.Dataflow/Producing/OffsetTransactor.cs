namespace Confluent.Kafka.Dataflow.Producing
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;

    class OffsetTransactor<T, TKey, TValue> : ITransactor<T>
    {
        readonly IConsumer<TKey, TValue> consumer;

        public OffsetTransactor(IConsumer<TKey, TValue> consumer)
        {
            this.consumer = consumer;
        }

        public async Task<IEnumerable<KeyValuePair<int, TopicPartitionOffset>>> Send(
            IEnumerable<T> items,
            IEnumerable<TopicPartitionOffset> offsets)
        {
            await Task.Factory.StartNew(
                obj =>
                {
                    var (c, o) = ((IConsumer<TKey, TValue>, IEnumerable<TopicPartitionOffset>))obj!;
                    c.Commit(o.ToArray());
                },
                (this.consumer, offsets),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Current)
                .ConfigureAwait(false);

            return Array.Empty<KeyValuePair<int, TopicPartitionOffset>>();
        }
    }
}
