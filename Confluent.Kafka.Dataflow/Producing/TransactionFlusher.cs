namespace Confluent.Kafka.Dataflow.Producing
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class TransactionFlusher<T>
    {
        readonly ITransactor<T> transactor;
        readonly TimeSpan interval;
        readonly IProducerConsumerCollection<TopicPartitionOffset>? offsetSource;
        readonly Func<T, int>? offsetMapping;

        public TransactionFlusher(
            ITransactor<T> transactor,
            TimeSpan interval,
            IProducerConsumerCollection<TopicPartitionOffset>? offsetSource,
            Func<T, int>? offsetMapping)
        {
            this.transactor = transactor;
            this.interval = interval;
            this.offsetSource = offsetSource;
            this.offsetMapping = offsetMapping;
        }

        public event Action<T, IReadOnlyList<TopicPartitionOffset>>? OnDelivered;

        public async Task Flush(IReceivableSourceBlock<T> source, CancellationToken cancellationToken)
        {
            while (!source.Completion.IsCompleted)
            {
                try
                {
                    await Task.Delay(this.interval, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    source.Complete();
                }

                if (source.TryReceiveAll(out var items))
                {
                    var offsets = new List<KeyValuePair<T, TopicPartitionOffset>>();
                    var published = await this.transactor.Send(items, GetSourceOffsets()).ConfigureAwait(false);

                    offsets.AddRange(published);
                    var sorted = offsets.ToLookup(x => x.Key, x => x.Value);

                    foreach (var item in items)
                    {
                        this.OnDelivered?.Invoke(item, sorted[item].ToArray());
                    }

                    IEnumerable<TopicPartitionOffset> GetSourceOffsets()
                    {
                        if (this.offsetSource == null)
                        {
                            yield break;
                        }

                        var positions = new Dictionary<TopicPartition, Offset>();

                        foreach (var item in items)
                        {
                            var n = this.offsetMapping?.Invoke(item) ?? 1;

                            while (n-- > 0)
                            {
                                if (!this.offsetSource.TryTake(out var tpo))
                                {
                                    throw new InvalidOperationException("No offset stored!");
                                }

                                offsets.Add(new KeyValuePair<T, TopicPartitionOffset>(item, tpo));
                                positions[tpo.TopicPartition] = tpo.Offset;
                            }
                        }

                        foreach (var kvp in positions)
                        {
                            yield return new TopicPartitionOffset(kvp.Key, kvp.Value + 1);
                        }
                    }
                }
            }
        }
    }
}
