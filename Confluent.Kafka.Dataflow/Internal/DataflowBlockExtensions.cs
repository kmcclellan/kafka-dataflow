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
            new LazySource<T>(GetTrigger(source, action));

        public static ITargetBlock<T> BeginWith<T>(this ITargetBlock<T> target, Lazy<Task> action) =>
            new LazyTarget<T>(GetTrigger(target, action)).ContinueWith(action);

        public static ITargetBlock<T> ContinueWith<T>(this ITargetBlock<T> target, Lazy<Task> action) =>
            new ExtendedTarget<T>(target, ContinueWithAsync(target.Completion, action));

        static Func<T> GetTrigger<T>(T block, Lazy<Task> action)
            where T : IDataflowBlock
        {
            return () =>
            {
                action.Value.ContinueWith(
                    (t, obj) =>
                    {
                        if (t.IsFaulted)
                        {
                            ((IDataflowBlock)obj).Fault(t.Exception);
                        }
                        else
                        {
                            ((IDataflowBlock)obj).Complete();
                        }
                    },
                    block,
                    default,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);

                return block;
            };
        }

        static async Task ContinueWithAsync(Task task, Lazy<Task> action)
        {
            await task;
            await action.Value;
        }

        class LazySource<T> : LazyBlock<IReceivableSourceBlock<T>>, IReceivableSourceBlock<T>
        {
            public LazySource(Func<IReceivableSourceBlock<T>> factory)
                : base(factory)
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
            public LazyTarget(Func<ITargetBlock<T>> factory)
                : base(factory)
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
            private readonly TaskCompletionSource<byte> completionSource = new();
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
                        this.value = factory();
                        this.value.Completion.ContinueWith(
                            (t, obj) =>
                            {
                                if (t.IsFaulted)
                                {
                                    ((TaskCompletionSource<byte>)obj).TrySetException(t.Exception);
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

        class ExtendedTarget<T> : ITargetBlock<T>
        {
            private readonly ITargetBlock<T> target;

            public ExtendedTarget(ITargetBlock<T> target, Task completion)
            {
                this.target = target;
                this.Completion = completion;
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
}
