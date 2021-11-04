namespace Confluent.Kafka.Dataflow.Producing
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;

    class MessageTransactor<T> : ITransactor<T>
    {
        readonly IProducer<Null, Null> producer;
        readonly IEnumerable<IMessageSender<T>> senders;
        readonly IConsumerGroupMetadata? consumerMetadata;

        public MessageTransactor(
            IProducer<Null, Null> producer,
            IEnumerable<IMessageSender<T>> senders,
            IConsumerGroupMetadata? consumerMetadata)
        {
            this.producer = producer;
            this.senders = senders;
            this.consumerMetadata = consumerMetadata;
        }

        public async Task<IEnumerable<KeyValuePair<int, TopicPartitionOffset>>> Send(
            IEnumerable<T> items,
            IEnumerable<TopicPartitionOffset> offsets)
        {
            this.producer.BeginTransaction();

            var produced = await Task.WhenAll(SendAll()).ConfigureAwait(false);

            IEnumerable<Task<KeyValuePair<int, TopicPartitionOffset>>> SendAll()
            {
                var enumerator = items.GetEnumerator();
                for (var index = 0; enumerator.MoveNext(); index++)
                {
                    foreach (var sender in this.senders)
                    {
                        foreach (var task in sender.Send(enumerator.Current))
                        {
                            yield return Wrap(index, task);
                        }
                    }
                }
            }

            static async Task<KeyValuePair<int, TopicPartitionOffset>> Wrap(int index, Task<TopicPartitionOffset> task)
            {
                return new KeyValuePair<int, TopicPartitionOffset>(index, await task.ConfigureAwait(false));
            }

            await Task.Factory.StartNew(
                obj =>
                {
                    var (t, o) = ((MessageTransactor<T>, IEnumerable<TopicPartitionOffset>))obj!;
                    t.FinishSync(o);
                },
                (this, offsets),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Current)
                .ConfigureAwait(false);

            return produced;
        }

        private void FinishSync(IEnumerable<TopicPartitionOffset> offsets)
        {
            if (this.consumerMetadata != null)
            {
                this.producer.SendOffsetsToTransaction(
                    offsets.ToArray(),
                    this.consumerMetadata,
                    Timeout.InfiniteTimeSpan);
            }

            this.producer.CommitTransaction();
        }
    }
}
