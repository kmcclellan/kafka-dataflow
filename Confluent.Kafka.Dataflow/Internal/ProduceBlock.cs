namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ProduceBlock<TKey, TValue> : ITargetBlock<Message<TKey, TValue>>
    {
        readonly ITargetBlock<Message<TKey, TValue>> target;
        readonly Action<DeliveryReport<TKey, TValue>> deliveryHandler;

        readonly IProducer<TKey, TValue> producer;
        readonly TopicPartition topicPartition;
        readonly ProduceBlockOptions options;

        public ProduceBlock(IProducer<TKey, TValue> producer, TopicPartition topicPartition, ProduceBlockOptions options)
        {
            this.producer = producer;
            this.topicPartition = topicPartition;
            this.options = options;

            this.deliveryHandler = this.OnDelivery;
            this.target = new ActionBlock<Message<TKey, TValue>>(this.Produce, options);

            var flushTask = this.target.Completion.ContinueWith(
                (task, obj) =>
                {
                    var block = (ProduceBlock<TKey, TValue>)obj!;
                    block.producer.Flush(block.options.CancellationToken);
                },
                this,
                this.options.CancellationToken,
                TaskContinuationOptions.OnlyOnRanToCompletion,
                this.options.TaskScheduler);

            this.Completion = Task.Factory.ContinueWhenAll(
                new[] { this.target.Completion, flushTask },
                t => t[0].IsFaulted ? t[0] : t[1],
                CancellationToken.None,
                TaskContinuationOptions.None,
                this.options.TaskScheduler).Unwrap();
        }

        public Task Completion { get; }

        public DataflowMessageStatus OfferMessage(
            DataflowMessageHeader messageHeader,
            Message<TKey, TValue> messageValue,
            ISourceBlock<Message<TKey, TValue>>? source,
            bool consumeToAccept)
        {
            return this.target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        void Produce(Message<TKey, TValue> message) =>
            this.producer.Produce(this.topicPartition, message, this.deliveryHandler);

        void OnDelivery(DeliveryReport<TKey, TValue> report)
        {
            if (report.Error.IsError)
            {
                this.target.Fault(new ProduceException<TKey, TValue>(report.Error, report));
            }
            else
            {
                this.options.OffsetHandler?.Invoke(report.TopicPartitionOffset);
            }
        }

        public void Complete() => this.target.Complete();

        public void Fault(Exception exception) => this.target.Fault(exception);
    }
}
