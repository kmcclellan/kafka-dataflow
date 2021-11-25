namespace Confluent.Kafka.Dataflow.Consuming
{
    using System;
    using Confluent;

    class OffsetBuffer<TKey, TValue>
    {
        public void Add(Message<TKey, TValue> message, TopicPartitionOffset offset)
        {
            throw new NotImplementedException();
        }

        public TopicPartitionOffset Retrieve(Message<TKey, TValue> message)
        {
            throw new NotImplementedException();
        }
    }
}
