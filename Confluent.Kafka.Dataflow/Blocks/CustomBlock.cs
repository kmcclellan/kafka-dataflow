namespace Confluent.Kafka.Dataflow.Blocks
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class CustomBlock<T> : IPropagatorBlock<T, T>
    {
        readonly Func<BufferBlock<T>, CancellationToken, Task> executor;

        readonly BufferBlock<T> buffer;
        readonly CancellationTokenSource execution = new();

        public CustomBlock(Func<BufferBlock<T>, CancellationToken, Task> executor, DataflowBlockOptions options)
        {
            this.executor = executor;

            this.buffer = new(options);
            this.buffer.Completion.ContinueWith(
                (_, obj) => ((CancellationTokenSource)obj!).Cancel(),
                this.execution,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.NotOnRanToCompletion,
                options.TaskScheduler);

            var executeTask = Task.Factory.StartNew(
                obj => ((CustomBlock<T>)obj!).Execute(),
                this,
                this.execution.Token,
                TaskCreationOptions.None,
                options.TaskScheduler)
                .Unwrap();

            this.Completion = executeTask.ContinueWith(
                (task, obj) =>
                {
                    var block = (IDataflowBlock)obj!;

                    if (task.IsFaulted)
                    {
                        block.Fault(task.Exception!);
                    }
                    else
                    {
                        block.Complete();
                    }

                    return block.Completion;
                },
                this.buffer,
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
            return ((ITargetBlock<T>)this.buffer).OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions) =>
            ((ISourceBlock<T>)this.buffer).LinkTo(target, linkOptions);

        public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed)
        {
            return ((ISourceBlock<T>)this.buffer).ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
            ((ISourceBlock<T>)this.buffer).ReserveMessage(messageHeader, target);

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
            ((ISourceBlock<T>)this.buffer).ReleaseReservation(messageHeader, target);

        public void Complete() => this.execution.Cancel();

        public void Fault(Exception exception) => ((IDataflowBlock)this.buffer).Fault(exception);

        private Task Execute() => this.executor(this.buffer, this.execution.Token);
    }
}
