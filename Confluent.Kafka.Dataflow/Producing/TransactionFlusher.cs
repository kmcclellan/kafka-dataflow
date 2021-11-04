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
                    var itemOffsets = Enumerable.Range(0, items.Count)
                        .Select(_ => new List<TopicPartitionOffset>())
                        .ToArray();

                    IEnumerable<TopicPartitionOffset> commitOffsets;

                    if (this.offsetSource == null)
                    {
                        commitOffsets = Array.Empty<TopicPartitionOffset>();
                    }
                    else
                    {
                        var commitPositions = new Dictionary<TopicPartition, Offset>();

                        for (var i = 0; i < items.Count; i++)
                        {
                            var n = this.offsetMapping?.Invoke(items[i]) ?? 1;

                            while (n-- > 0)
                            {
                                if (!this.offsetSource.TryTake(out var tpo))
                                {
                                    throw new InvalidOperationException("No offset stored!");
                                }

                                itemOffsets[i].Add(tpo);
                                commitPositions[tpo.TopicPartition] = tpo.Offset;
                            }
                        }

                        commitOffsets = commitPositions.Select(
                            kvp => new TopicPartitionOffset(kvp.Key, kvp.Value + 1));
                    }

                    var published = await this.transactor.Send(items, commitOffsets).ConfigureAwait(false);

                    foreach (var kvp in published)
                    {
                        itemOffsets[kvp.Key].Add(kvp.Value);
                    }

                    for (var i = 0; i < items.Count; i++)
                    {
                        this.OnDelivered?.Invoke(items[i], itemOffsets[i]);
                    }
                }
            }
        }
    }
}
