namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class CustomSource<T> : IReceivableSourceBlock<T>
    {
        private readonly IReceivableSourceBlock<T> source;
        private readonly TaskCompletionSource<byte> completionSource;
        private readonly Action? start;

        public CustomSource(IReceivableSourceBlock<T> source, TaskCompletionSource<byte> completionSource, Action? start = null)
        {
            this.source = source;
            this.completionSource = completionSource;
            this.start = start;
        }

        public Task Completion => this.source.Completion;

        public bool TryReceive(Predicate<T>? filter, [MaybeNullWhen(false)] out T item) =>
            this.source.TryReceive(filter, out item);

        public bool TryReceiveAll([NotNullWhen(true)] out IList<T>? items) => this.source.TryReceiveAll(out items);

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
        {
            this.start?.Invoke();
            return this.source.LinkTo(target, linkOptions);
        }

        public T? ConsumeMessage(
            DataflowMessageHeader messageHeader,
            ITargetBlock<T> target,
            out bool messageConsumed)
        {
            return this.source.ConsumeMessage(messageHeader, target, out messageConsumed);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
            this.source.ReserveMessage(messageHeader, target);

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
            this.source.ReleaseReservation(messageHeader, target);

        public void Complete() => this.completionSource.TrySetResult(default);

        public void Fault(Exception exception) => this.completionSource.TrySetException(exception);
    }
}
