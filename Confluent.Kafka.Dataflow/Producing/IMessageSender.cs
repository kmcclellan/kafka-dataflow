namespace Confluent.Kafka.Dataflow.Producing
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    interface IMessageSender<T>
    {
        IEnumerable<Task<TopicPartitionOffset>> Send(T item);
    }
}
