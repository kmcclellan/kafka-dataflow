namespace Confluent.Kafka.Dataflow.Internal
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ProducerBlockFactory<TKey, TValue>
    {
        private readonly IProducer<TKey, TValue> producer;

        public ProducerBlockFactory(IProducer<TKey, TValue> producer)
        {
            this.producer = producer;
        }

        public ITargetBlock<KeyValuePair<TKey, TValue>> GetTarget(TopicPartition topicPartition)
        {
            // No delivery handler support at this time. (Implement as a separate source block.)
            var processor = new ActionBlock<KeyValuePair<TKey, TValue>>(
                kvp => this.producer.Produce(
                    topicPartition,
                    new Message<TKey, TValue> { Key = kvp.Key, Value = kvp.Value }));

            return new CustomTarget<KeyValuePair<TKey, TValue>>(
                processor,
                this.ContinueWithFlush(processor.Completion));
        }

        async Task ContinueWithFlush(Task completion)
        {
            await completion;
            this.producer.Flush(Timeout.InfiniteTimeSpan);
        }
    }
}
