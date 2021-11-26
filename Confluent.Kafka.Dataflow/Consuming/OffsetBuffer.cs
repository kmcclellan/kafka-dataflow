namespace Confluent.Kafka.Dataflow.Consuming
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Confluent;

    class OffsetBuffer<TKey, TValue>
    {
        readonly ConcurrentDictionary<Message<TKey, TValue>, TopicPartitionOffset> offsets = new();
        readonly Dictionary<TopicPartition, Offset> positions = new();

        public void Add(Message<TKey, TValue> message, TopicPartitionOffset offset)
        {
            if (!this.offsets.TryAdd(message, offset))
            {
                throw new InvalidOperationException("Message already added.");
            }
        }

        public TopicPartitionOffset Retrieve(Message<TKey, TValue> message)
        {
            if (!this.offsets.TryRemove(message, out var tpo))
            {
                throw new InvalidOperationException("No offset added for message.");
            }

            if (this.positions.TryGetValue(tpo.TopicPartition, out var previous) && previous > tpo.Offset)
            {
                throw new InvalidOperationException("Offsets retrieved out of order for partition.");
            }

            this.positions[tpo.TopicPartition] = tpo.Offset;
            return tpo;
        }
    }
}
