namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    static class DataflowBlockExtensions
    {
        public static CancellationTokenSource GetCompletionToken(this IDataflowBlock block)
        {
            var result = new CancellationTokenSource();

            block.Completion.ContinueWith(
                (t, obj) => ((CancellationTokenSource)obj).Cancel(),
                result,
                default,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);

            return result;
        }

        public static IReceivableSourceBlock<T> BeginWith<T>(this IReceivableSourceBlock<T> source, Lazy<Task> action) =>
            new LazySource<T>(GetTrigger(source, action)).ContinueWith(action);

        public static ITargetBlock<T> BeginWith<T>(this ITargetBlock<T> target, Lazy<Task> action) =>
            new LazyTarget<T>(GetTrigger(target, action)).ContinueWith(action);

        public static IReceivableSourceBlock<T> ContinueWith<T>(this IReceivableSourceBlock<T> source, Lazy<Task> action) =>
            new ExtendedSource<T>(source, ContinueWithAsync(source.Completion, action));

        public static ITargetBlock<T> ContinueWith<T>(this ITargetBlock<T> target, Lazy<Task> action) =>
            new ExtendedTarget<T>(target, ContinueWithAsync(target.Completion, action));

        static Func<T> GetTrigger<T>(T obj, Lazy<Task> action)
        {
            return () =>
            {
                _ = action.Value;
                return obj;
            };
        }

        static async Task ContinueWithAsync(Task task, Lazy<Task> action)
        {
            await task;
            await action.Value;
        }

        class LazySource<T> : LazyBlock<IReceivableSourceBlock<T>>, IReceivableSourceBlock<T>
        {
            public LazySource(Func<IReceivableSourceBlock<T>> factory) : base(factory)
            {
            }

            public bool TryReceive(Predicate<T>? filter, [MaybeNullWhen(false)] out T item) =>
                this.Value.TryReceive(out item);

            public bool TryReceiveAll([NotNullWhen(true)] out IList<T>? items) =>
                this.Value.TryReceiveAll(out items);

            public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions) =>
                this.Value.LinkTo(target, linkOptions);

            public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed) =>
                this.Value.ConsumeMessage(messageHeader, target, out messageConsumed);

            public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
                this.Value.ReserveMessage(messageHeader, target);

            public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
                this.Value.ReleaseReservation(messageHeader, target);
        }

        class LazyTarget<T> : LazyBlock<ITargetBlock<T>>, ITargetBlock<T>
        {
            public LazyTarget(Func<ITargetBlock<T>> factory) : base(factory)
            {
            }

            public DataflowMessageStatus OfferMessage(
                DataflowMessageHeader messageHeader,
                T messageValue,
                ISourceBlock<T>? source,
                bool consumeToAccept)
            {
                return this.Value.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
            }
        }

        abstract class LazyBlock<T> : IDataflowBlock
            where T : IDataflowBlock
        {
            readonly Func<T> factory;
            readonly TaskCompletionSource<byte> completionSource = new();
            T? value;

            public LazyBlock(Func<T> factory)
            {
                this.factory = factory;
            }

            public Task Completion => this.completionSource.Task;

            protected T Value
            {
                get
                {
                    if (this.value == null)
                    {
                        this.value = this.factory();
                        this.value.Completion.ContinueWith(
                            (task, obj) =>
                            {
                                if (task.IsFaulted)
                                {
                                    ((TaskCompletionSource<byte>)obj).TrySetException(task.Exception);
                                }
                                else
                                {
                                    ((TaskCompletionSource<byte>)obj).TrySetResult(default);
                                }
                            },
                            this.completionSource,
                            TaskScheduler.Default);
                    }

                    return this.value;
                }
            }

            public void Complete() => this.Value.Complete();

            public void Fault(Exception exception) => this.Value.Fault(exception);
        }

        class ExtendedSource<T> : ExtendedBlock, IReceivableSourceBlock<T>
        {
            readonly IReceivableSourceBlock<T> source;

            public ExtendedSource(IReceivableSourceBlock<T> source, Task completion)
                : base(source, completion)
            {
                this.source = source;
            }

            public bool TryReceive(Predicate<T>? filter, [MaybeNullWhen(false)] out T item) =>
                this.source.TryReceive(out item);

            public bool TryReceiveAll([NotNullWhen(true)] out IList<T>? items) =>
                this.source.TryReceiveAll(out items);

            public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions) =>
                this.source.LinkTo(target, linkOptions);

            public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed) =>
                this.source.ConsumeMessage(messageHeader, target, out messageConsumed);

            public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
                this.source.ReserveMessage(messageHeader, target);

            public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target) =>
                this.source.ReleaseReservation(messageHeader, target);
        }

        class ExtendedTarget<T> : ExtendedBlock, ITargetBlock<T>
        {
            readonly ITargetBlock<T> target;

            public ExtendedTarget(ITargetBlock<T> target, Task completion)
                : base(target, completion)
            {
                this.target = target;
            }

            public DataflowMessageStatus OfferMessage(
                DataflowMessageHeader messageHeader,
                T messageValue,
                ISourceBlock<T>? source,
                bool consumeToAccept)
            {
                return this.target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
            }
        }

        class ExtendedBlock : IDataflowBlock
        {
            readonly IDataflowBlock block;

            public ExtendedBlock(IDataflowBlock block, Task completion)
            {
                this.block = block;

                // Propagate faults upstream.
                completion.ContinueWith(
                    (task, obj) => ((IDataflowBlock)obj!).Fault(task.Exception!),
                    block,
                    default,
                    TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);

                this.Completion = Task.WhenAll(block.Completion, completion);
            }

            public Task Completion { get; }

            public void Complete() => this.block.Complete();

            public void Fault(Exception exception) => this.block.Fault(exception);
        }
    }
}
