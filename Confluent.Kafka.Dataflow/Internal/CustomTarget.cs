namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class CustomTarget<T> : ITargetBlock<T>
    {
        private readonly ITargetBlock<T> target;

        public CustomTarget(ITargetBlock<T> target, Task completion)
        {
            this.target = target;
            this.Completion = completion;
        }

        public Task Completion { get; private set; }

        public DataflowMessageStatus OfferMessage(
            DataflowMessageHeader messageHeader,
            T messageValue,
            ISourceBlock<T>? source,
            bool consumeToAccept)
        {
            return this.target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void Complete() => this.target.Complete();

        public void Fault(Exception exception) => this.target.Fault(exception);
    }
}
