namespace Confluent.Kafka.Dataflow
{
    using System.Threading.Tasks.Dataflow;

    /// <summary>
    /// Dataflow block options for consuming from Kafka.
    /// </summary>
    public class ConsumeBlockOptions : DataflowBlockOptions
    {
        /// <summary>
        /// Gets or sets the target for consumed message offsets.
        /// </summary>
        public ITargetBlock<TopicPartitionOffset>? OffsetTarget { get; set; }
    }
}
