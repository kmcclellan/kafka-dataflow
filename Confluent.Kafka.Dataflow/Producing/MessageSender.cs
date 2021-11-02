namespace Confluent.Kafka.Dataflow.Producing
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Confluent.Kafka;

    class MessageSender<T, TKey, TValue> : IMessageSender<T>
    {
        readonly IProducer<TKey, TValue> producer;
        readonly Func<T, IEnumerable<Message<TKey, TValue>>> mapping;
        readonly TopicPartition topicPartition;

        public MessageSender(
            IProducer<TKey, TValue> producer,
            Func<T, IEnumerable<Message<TKey, TValue>>> mapping,
            TopicPartition topicPartition)
        {
            this.producer = producer;
            this.mapping = mapping;
            this.topicPartition = topicPartition;
        }

        public IEnumerable<Task<TopicPartitionOffset>> Send(T item)
        {
            foreach (var message in this.mapping(item))
            {
                yield return this.ProduceOffset(message);
            }
        }

        private async Task<TopicPartitionOffset> ProduceOffset(Message<TKey, TValue> message)
        {
            var result = await this.producer.ProduceAsync(this.topicPartition, message).ConfigureAwait(false);
            return result.TopicPartitionOffset;
        }
    }
}
