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

        public async Task<IEnumerable<KeyValuePair<T, TopicPartitionOffset>>> Send(
            IEnumerable<T> items,
            IEnumerable<TopicPartitionOffset> offsets)
        {
            this.producer.BeginTransaction();

            var produced = await Task.WhenAll(SendAll()).ConfigureAwait(false);

            IEnumerable<Task<KeyValuePair<T, TopicPartitionOffset>>> SendAll()
            {
                foreach (var item in items)
                {
                    foreach (var sender in this.senders)
                    {
                        foreach (var task in sender.Send(item))
                        {
                            yield return Wrap(item, task);
                        }
                    }
                }
            }

            static async Task<KeyValuePair<T, TopicPartitionOffset>> Wrap(T item, Task<TopicPartitionOffset> task)
            {
                return new KeyValuePair<T, TopicPartitionOffset>(item, await task.ConfigureAwait(false));
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
