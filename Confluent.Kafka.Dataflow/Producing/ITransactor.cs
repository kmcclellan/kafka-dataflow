namespace Confluent.Kafka.Dataflow.Producing
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    interface ITransactor<T>
    {
        Task<IEnumerable<KeyValuePair<T, TopicPartitionOffset>>> Send(
            IEnumerable<T> items,
            IEnumerable<TopicPartitionOffset> offsets);
    }
}
