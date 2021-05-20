namespace Confluent.Kafka.Dataflow.Internal
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    class ConsumeBlock<TKey, TValue> : ISourceBlock<Message<TKey, TValue>>, IDisposable
    {
        readonly TransformManyBlock<byte, ConsumeResult<TKey, TValue>> source;
        readonly ConcurrentDictionary<ITargetBlock<Message<TKey, TValue>>, TargetAdapter> targets = new();
        readonly CancellationTokenSource stoppingSource;
        readonly TaskCompletionSource<byte> completionSource = new();

        readonly IConsumer<TKey, TValue> consumer;
        readonly ConsumeBlockOptions options;

        int count;

        public ConsumeBlock(IConsumer<TKey, TValue> consumer, ConsumeBlockOptions options)
        {
            this.consumer = consumer;
            this.options = options;

            this.stoppingSource = CancellationTokenSource.CreateLinkedTokenSource(this.options.CancellationToken);

            this.source = new(
                _ => this.Load(this.stoppingSource.Token),
                new ExecutionDataflowBlockOptions
                {
                    SingleProducerConstrained = true,
                    CancellationToken = this.stoppingSource.Token,
                    TaskScheduler = this.options.TaskScheduler,
                });

            this.source.Completion.ContinueWith(
                (task, obj) =>
                {
                    var block = (ConsumeBlock<TKey, TValue>)obj!;

                    if (task.IsFaulted)
                    {
                        block.completionSource.TrySetException(task.Exception!);
                    }
                    else
                    {
                        block.CompleteIfPossible();
                    }
                },
                this,
                this.options.TaskScheduler);

            this.source.Post(default);
        }

        public Task Completion => this.completionSource.Task;

        public IDisposable LinkTo(ITargetBlock<Message<TKey, TValue>> target, DataflowLinkOptions linkOptions)
        {
            return this.source.LinkTo(
                this.targets.GetOrAdd(target, new TargetAdapter(target, this)),
                linkOptions);
        }

        public Message<TKey, TValue>? ConsumeMessage(
            DataflowMessageHeader messageHeader,
            ITargetBlock<Message<TKey, TValue>> target,
            out bool messageConsumed)
        {
            var result = ((ISourceBlock<ConsumeResult<TKey, TValue>>)this.source)
                .ConsumeMessage(messageHeader, this.targets[target], out messageConsumed);

            if (messageConsumed && result != null)
            {
                this.OnConsumed(messageHeader, result.TopicPartitionOffset);
                return result.Message;
            }

            return null;
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<Message<TKey, TValue>> target) =>
            ((ISourceBlock<ConsumeResult<TKey, TValue>>)this.source).ReserveMessage(messageHeader, this.targets[target]);

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<Message<TKey, TValue>> target) =>
            ((ISourceBlock<ConsumeResult<TKey, TValue>>)this.source).ReleaseReservation(messageHeader, this.targets[target]);

        public void Complete()
        {
            this.source.Complete();
            this.stoppingSource.Cancel();
        }

        public void Fault(Exception exception)
        {
            ((IDataflowBlock)this.source).Fault(exception);
            this.stoppingSource.Cancel();
        }

        public void Dispose()
        {
            this.stoppingSource.Dispose();
        }

        IEnumerable<ConsumeResult<TKey, TValue>> Load(CancellationToken cancellationToken)
        {
            var count = this.count;

            while (count < this.options.BoundedCapacity ||
                this.options.BoundedCapacity == DataflowBlockOptions.Unbounded)
            {
                var result = this.consumer.Consume(cancellationToken);

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                count = Interlocked.Increment(ref this.count);
                yield return result;
            }
        }

        void OnConsumed(DataflowMessageHeader header, TopicPartitionOffset offset)
        {
            if (this.options.OffsetTarget != null &&
                this.options.OffsetTarget.OfferMessage(header, offset, null, false) != DataflowMessageStatus.Accepted)
            {
                throw new InvalidOperationException("Offset rejected by handler");
            }

            // Restart consuming if buffer was full.
            if (Interlocked.Decrement(ref this.count) + 1 == this.options.BoundedCapacity)
            {
                this.source.Post(default);
            }

            if (this.source.Completion.IsCompleted)
            {
                this.CompleteIfPossible();
            }
        }

        void CompleteIfPossible()
        {
            if (this.options.CancellationToken.IsCancellationRequested)
            {
                this.completionSource.TrySetCanceled(this.options.CancellationToken);
            }
            else if (this.count == 0)
            {
                this.completionSource.TrySetResult(default);
            }
        }

        class TargetAdapter : ITargetBlock<ConsumeResult<TKey, TValue>>
        {
            readonly ITargetBlock<Message<TKey, TValue>> messageTarget;
            readonly ConsumeBlock<TKey, TValue> source;

            public TargetAdapter(ITargetBlock<Message<TKey, TValue>> messageTarget, ConsumeBlock<TKey, TValue> source)
            {
                this.messageTarget = messageTarget;
                this.source = source;
            }

            public Task Completion => this.messageTarget.Completion;

            public DataflowMessageStatus OfferMessage(
                DataflowMessageHeader messageHeader,
                ConsumeResult<TKey, TValue> messageValue,
                ISourceBlock<ConsumeResult<TKey, TValue>>? source,
                bool consumeToAccept)
            {
                var result = this.messageTarget.OfferMessage(messageHeader, messageValue.Message, this.source, consumeToAccept);
                if (result == DataflowMessageStatus.Accepted && !consumeToAccept)
                {
                    this.source.OnConsumed(messageHeader, messageValue.TopicPartitionOffset);
                }

                return result;
            }

            public void Complete() => this.messageTarget.Complete();

            public void Fault(Exception exception) => this.messageTarget.Fault(exception);
        }
    }
}
