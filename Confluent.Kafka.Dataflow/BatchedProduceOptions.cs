namespace Confluent.Kafka.Dataflow
{
    using System;

    /// <summary>
    /// Options for producing batches of messages/offsets to Kafka.
    /// </summary>
    public class BatchedProduceOptions
    {
        /// <summary>
        /// Gets or sets the time between Kafka transactions.
        /// </summary>
        public TimeSpan TransactionInterval { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Gets or sets the maximum number of uncommitted batches buffered by producer blocks.
        /// </summary>
        /// <remarks>
        /// Default is <c>null</c> (unbounded).
        /// </remarks>
        public int? BoundedCapacity { get; set; }
    }
}
