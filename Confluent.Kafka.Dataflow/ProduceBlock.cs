namespace Confluent.Kafka.Dataflow;

using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

/// <summary>
/// A producer block that is a target for Kafka messages and a source for delivery reports.
/// </summary>
/// <typeparam name="TKey">The producer key type.</typeparam>
/// <typeparam name="TValue">The producer value type.</typeparam>
public class ProduceBlock<TKey, TValue> : IPropagatorBlock<Message<TKey, TValue>, DeliveryReport<TKey, TValue>>
{
    /// <summary>
    /// Initializes the block.
    /// </summary>
    /// <param name="producer">The Kafka producer client.</param>
    /// <param name="target">The target Kafka topic/partition (<see cref="Partition.Any"/> for automatic).</param>
    /// <param name="options">Block options for producing.</param>
    public ProduceBlock(IProducer<TKey, TValue> producer, TopicPartition target, DataflowBlockOptions? options = null)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task Completion => Task.CompletedTask;

    /// <inheritdoc/>
    public DataflowMessageStatus OfferMessage(
        DataflowMessageHeader messageHeader,
        Message<TKey, TValue> messageValue,
        ISourceBlock<Message<TKey, TValue>>? source,
        bool consumeToAccept)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public IDisposable LinkTo(ITargetBlock<DeliveryReport<TKey, TValue>> target, DataflowLinkOptions linkOptions)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public DeliveryReport<TKey, TValue>? ConsumeMessage(
        DataflowMessageHeader messageHeader,
        ITargetBlock<DeliveryReport<TKey, TValue>> target,
        out bool messageConsumed)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<DeliveryReport<TKey, TValue>> target)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<DeliveryReport<TKey, TValue>> target)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Complete()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Fault(Exception exception)
    {
        throw new NotImplementedException();
    }
}
