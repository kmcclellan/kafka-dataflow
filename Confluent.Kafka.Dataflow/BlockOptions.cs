namespace Confluent.Kafka.Dataflow
{
    using System;
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

    /// <summary>
    /// Dataflow block options for handling processed Kafka offsets.
    /// </summary>
    public class OffsetBlockOptions : ExecutionDataflowBlockOptions
    {
    }

    /// <summary>
    /// Dataflow block options for producing to Kafka.
    /// </summary>
    public class ProduceBlockOptions : ExecutionDataflowBlockOptions
    {
        /// <summary>
        /// Gets or sets the handler for produced message offsets.
        /// </summary>
        public Action<TopicPartitionOffset>? OffsetHandler { get; set; }
    }
}
