namespace Confluent.Kafka.Dataflow.Blocks
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ContinueBlock<T> : ITargetBlock<T>
    {
        readonly ITargetBlock<T> target;

        public ContinueBlock(ITargetBlock<T> target, Func<Task> continuation, DataflowBlockOptions options)
        {
            this.target = target;
            this.Completion = this.target.Completion.ContinueWith(
                (task, obj) =>
                {
                    if (task.IsFaulted)
                    {
                        return task;
                    }

                    return ((Func<Task>)obj!).Invoke();
                },
                continuation,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                options.TaskScheduler)
                .Unwrap();
        }

        public Task Completion { get; }

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
